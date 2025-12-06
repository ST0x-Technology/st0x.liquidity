//! Inventory view for tracking cross-venue asset positions.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Add, Sub};

use super::venue_balance::{InventoryError, VenueBalance};
use crate::equity_redemption::EquityRedemptionEvent;
use crate::position::PositionEvent;
use crate::shares::{ArithmeticError, FractionalShares, HasZero};
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
use st0x_broker::{Direction, Symbol};

/// Error type for inventory view operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub(crate) enum InventoryViewError {
    #[error("unknown symbol: {0}")]
    UnknownSymbol(Symbol),
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
}

/// Imbalance requiring rebalancing action.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Imbalance<T> {
    /// Too much onchain - triggers movement to offchain.
    TooMuchOnchain { excess: T },
    /// Too much offchain - triggers movement to onchain.
    TooMuchOffchain { excess: T },
}

/// Threshold configuration for imbalance detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    target: Decimal,
    /// Deviation from target that triggers rebalancing.
    deviation: Decimal,
}

/// Inventory at a pair of venues (onchain/offchain).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Inventory<T> {
    onchain: VenueBalance<T>,
    offchain: VenueBalance<T>,
    last_rebalancing: Option<DateTime<Utc>>,
}

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + std::ops::Mul<Decimal, Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + Into<Decimal>,
{
    /// Returns the ratio of onchain to total inventory.
    /// Returns `None` if total is zero.
    fn ratio(&self) -> Option<Decimal> {
        let onchain: Decimal = self.onchain.total().ok()?.into();
        let offchain: Decimal = self.offchain.total().ok()?.into();
        let total = onchain + offchain;

        if total.is_zero() {
            return None;
        }

        Some(onchain / total)
    }

    fn has_inflight(&self) -> bool {
        self.onchain.has_inflight() || self.offchain.has_inflight()
    }

    /// Detects imbalance based on threshold configuration.
    /// Returns `None` if balanced, has inflight operations, or total is zero.
    fn detect_imbalance(&self, threshold: &ImbalanceThreshold) -> Option<Imbalance<T>> {
        if self.has_inflight() {
            return None;
        }

        let ratio = self.ratio()?;
        let lower = threshold.target - threshold.deviation;
        let upper = threshold.target + threshold.deviation;

        if ratio < lower {
            let onchain = self.onchain.total().ok()?;
            let offchain = self.offchain.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (target_onchain - onchain).ok()?;

            Some(Imbalance::TooMuchOffchain { excess })
        } else if ratio > upper {
            let onchain = self.onchain.total().ok()?;
            let offchain = self.offchain.total().ok()?;
            let total = (onchain + offchain).ok()?;
            let target_onchain = (total * threshold.target).ok()?;
            let excess = (onchain - target_onchain).ok()?;

            Some(Imbalance::TooMuchOnchain { excess })
        } else {
            None
        }
    }
}

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero,
{
    fn add_onchain_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            onchain: self.onchain.add_available(amount)?,
            ..self
        })
    }

    fn remove_onchain_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            onchain: self.onchain.remove_available(amount)?,
            ..self
        })
    }

    fn add_offchain_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.add_available(amount)?,
            ..self
        })
    }

    fn remove_offchain_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.remove_available(amount)?,
            ..self
        })
    }

    fn move_offchain_to_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.move_to_inflight(amount)?,
            ..self
        })
    }

    fn transfer_offchain_inflight_to_onchain(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.confirm_inflight(amount)?,
            onchain: self.onchain.add_available(amount)?,
            ..self
        })
    }

    fn cancel_offchain_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.cancel_inflight(amount)?,
            ..self
        })
    }

    fn move_onchain_to_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            onchain: self.onchain.move_to_inflight(amount)?,
            ..self
        })
    }

    fn transfer_onchain_inflight_to_offchain(self, amount: T) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            onchain: self.onchain.confirm_inflight(amount)?,
            offchain: self.offchain.add_available(amount)?,
            ..self
        })
    }

    fn with_last_rebalancing(self, timestamp: DateTime<Utc>) -> Self {
        Self {
            last_rebalancing: Some(timestamp),
            ..self
        }
    }
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
}

impl InventoryView {
    fn update_equity(
        self,
        symbol: &Symbol,
        f: impl FnOnce(
            Inventory<FractionalShares>,
        ) -> Result<Inventory<FractionalShares>, InventoryError<FractionalShares>>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let inventory = self
            .equities
            .get(symbol)
            .ok_or_else(|| InventoryViewError::UnknownSymbol(symbol.clone()))?;

        let updated = f(inventory.clone())?;

        let mut equities = self.equities;
        equities.insert(symbol.clone(), updated);

        Ok(Self {
            equities,
            last_updated: now,
            usdc: self.usdc,
        })
    }

    /// Applies a position event to update equity inventory.
    ///
    /// - `OnChainOrderFilled`: Buy adds to onchain available, Sell removes.
    /// - `OffChainOrderFilled`: Buy adds to offchain available, Sell removes.
    /// - Other events: Update `last_updated` only.
    pub(crate) fn apply_position_event(
        self,
        symbol: &Symbol,
        event: &PositionEvent,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        match event {
            PositionEvent::OnChainOrderFilled {
                amount, direction, ..
            } => {
                let amount = *amount;
                self.update_equity(
                    symbol,
                    |inv| match direction {
                        Direction::Buy => inv.add_onchain_available(amount),
                        Direction::Sell => inv.remove_onchain_available(amount),
                    },
                    now,
                )
            }

            PositionEvent::OffChainOrderFilled {
                shares_filled,
                direction,
                ..
            } => {
                let shares = *shares_filled;
                self.update_equity(
                    symbol,
                    |inv| match direction {
                        Direction::Buy => inv.add_offchain_available(shares),
                        Direction::Sell => inv.remove_offchain_available(shares),
                    },
                    now,
                )
            }

            PositionEvent::Initialized { .. }
            | PositionEvent::Migrated { .. }
            | PositionEvent::OffChainOrderPlaced { .. }
            | PositionEvent::OffChainOrderFailed { .. }
            | PositionEvent::ThresholdUpdated { .. } => Ok(Self {
                last_updated: now,
                ..self
            }),
        }
    }

    /// Applies a mint event to update equity inventory.
    ///
    /// - `MintRequested`: No balance change.
    /// - `MintRejected`: No balance change (rejection before acceptance).
    /// - `MintAccepted`: Move quantity from `offchain.available` to `offchain.inflight`.
    /// - `MintAcceptanceFailed`: Cancel inflight back to available (safe to restore).
    /// - `TokensReceived`: Remove from `offchain.inflight`, add to `onchain.available`.
    /// - `TokenReceiptFailed`: Keep inflight (funds location unknown).
    /// - `MintCompleted`: Update `last_rebalancing` timestamp.
    ///
    /// The `quantity` parameter is the mint quantity in `FractionalShares`, needed for
    /// events that modify balances but don't carry the quantity themselves.
    pub(crate) fn apply_mint_event(
        self,
        symbol: &Symbol,
        event: &TokenizedEquityMintEvent,
        quantity: FractionalShares,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        match event {
            // No balance changes for these events.
            TokenizedEquityMintEvent::MintRequested { .. }
            | TokenizedEquityMintEvent::MintRejected { .. }
            | TokenizedEquityMintEvent::TokenReceiptFailed { .. } => Ok(Self {
                last_updated: now,
                ..self
            }),

            TokenizedEquityMintEvent::MintAccepted { .. } => {
                self.update_equity(symbol, |inv| inv.move_offchain_to_inflight(quantity), now)
            }
            TokenizedEquityMintEvent::MintAcceptanceFailed { .. } => {
                self.update_equity(symbol, |inv| inv.cancel_offchain_inflight(quantity), now)
            }

            TokenizedEquityMintEvent::TokensReceived { .. } => self.update_equity(
                symbol,
                |inv| inv.transfer_offchain_inflight_to_onchain(quantity),
                now,
            ),

            TokenizedEquityMintEvent::MintCompleted { completed_at } => self.update_equity(
                symbol,
                |inv| Ok(inv.with_last_rebalancing(*completed_at)),
                now,
            ),
        }
    }

    /// Applies a redemption event to update equity inventory.
    ///
    /// - `TokensSent`: Move quantity from `onchain.available` to `onchain.inflight`.
    /// - `TokenSendFailed`: Keep inflight until manually resolved.
    /// - `Detected`: No balance change.
    /// - `DetectionFailed`: Keep inflight until manually resolved.
    /// - `Completed`: Remove from `onchain.inflight`, add to `offchain.available`.
    /// - `RedemptionRejected`: Keep inflight until manually resolved.
    ///
    /// The `quantity` parameter is the redemption quantity in `FractionalShares`, needed for
    /// events that modify balances but don't carry the quantity themselves.
    pub(crate) fn apply_redemption_event(
        self,
        symbol: &Symbol,
        event: &EquityRedemptionEvent,
        quantity: FractionalShares,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        match event {
            EquityRedemptionEvent::TokensSent { .. } => {
                self.update_equity(symbol, |inv| inv.move_onchain_to_inflight(quantity), now)
            }

            EquityRedemptionEvent::TokenSendFailed { .. } => {
                // Keep inflight until manually verified and resolved.
                Ok(Self {
                    last_updated: now,
                    ..self
                })
            }

            EquityRedemptionEvent::Detected { .. } => Ok(Self {
                last_updated: now,
                ..self
            }),

            EquityRedemptionEvent::DetectionFailed { .. } => {
                // Tokens were sent but detection failed - keep inflight until resolved.
                Ok(Self {
                    last_updated: now,
                    ..self
                })
            }

            EquityRedemptionEvent::Completed { completed_at } => self.update_equity(
                symbol,
                |inv| {
                    inv.transfer_onchain_inflight_to_offchain(quantity)
                        .map(|inv| inv.with_last_rebalancing(*completed_at))
                },
                now,
            ),

            EquityRedemptionEvent::RedemptionRejected { .. } => {
                // Rejection after detection - keep inflight until manually resolved.
                Ok(Self {
                    last_updated: now,
                    ..self
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{Address, TxHash, U256};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
    use crate::position::TradeId;
    use crate::threshold::ExecutionThreshold;
    use crate::tokenized_equity_mint::{IssuerRequestId, ReceiptId, TokenizationRequestId};

    fn shares(n: i64) -> FractionalShares {
        FractionalShares(Decimal::from(n))
    }

    fn venue(available: i64, inflight: i64) -> VenueBalance<FractionalShares> {
        VenueBalance::new(shares(available), shares(inflight))
    }

    fn inventory(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> Inventory<FractionalShares> {
        Inventory {
            onchain: venue(onchain_available, onchain_inflight),
            offchain: venue(offchain_available, offchain_inflight),
            last_rebalancing: None,
        }
    }

    fn threshold(target: &str, deviation: &str) -> ImbalanceThreshold {
        ImbalanceThreshold {
            target: target.parse().unwrap(),
            deviation: deviation.parse().unwrap(),
        }
    }

    #[test]
    fn ratio_returns_none_when_total_is_zero() {
        let inv = inventory(0, 0, 0, 0);
        assert!(inv.ratio().is_none());
    }

    #[test]
    fn ratio_returns_half_for_equal_split() {
        let inv = inventory(50, 0, 50, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn ratio_returns_one_when_all_onchain() {
        let inv = inventory(100, 0, 0, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::ONE);
    }

    #[test]
    fn ratio_returns_zero_when_all_offchain() {
        let inv = inventory(0, 0, 100, 0);
        assert_eq!(inv.ratio().unwrap(), Decimal::ZERO);
    }

    #[test]
    fn ratio_includes_inflight_in_total() {
        let inv = inventory(25, 25, 25, 25);
        assert_eq!(inv.ratio().unwrap(), Decimal::new(5, 1));
    }

    #[test]
    fn has_inflight_false_when_no_inflight() {
        let inv = inventory(50, 0, 50, 0);
        assert!(!inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_onchain_inflight() {
        let inv = inventory(50, 10, 50, 0);
        assert!(inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_offchain_inflight() {
        let inv = inventory(50, 0, 50, 10);
        assert!(inv.has_inflight());
    }

    #[test]
    fn has_inflight_true_when_both_inflight() {
        let inv = inventory(50, 10, 50, 10);
        assert!(inv.has_inflight());
    }

    #[test]
    fn detect_imbalance_returns_none_when_balanced() {
        let inv = inventory(50, 0, 50, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_has_inflight() {
        let inv = inventory(80, 10, 20, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_none_when_total_is_zero() {
        let inv = inventory(0, 0, 0, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_returns_too_much_onchain() {
        // 80 onchain, 20 offchain = 80% ratio, threshold is 50% +- 20%
        let inv = inventory(80, 0, 20, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inv.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 80, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOnchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_returns_too_much_offchain() {
        // 20 onchain, 80 offchain = 20% ratio, threshold is 50% +- 20%
        let inv = inventory(20, 0, 80, 0);
        let thresh = threshold("0.5", "0.2");

        let imbalance = inv.detect_imbalance(&thresh).unwrap();

        // Target is 50 onchain, current is 20, excess = 30
        assert_eq!(imbalance, Imbalance::TooMuchOffchain { excess: shares(30) });
    }

    #[test]
    fn detect_imbalance_at_upper_boundary_is_balanced() {
        // 70% ratio exactly at upper threshold (50% +- 20%)
        let inv = inventory(70, 0, 30, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn detect_imbalance_at_lower_boundary_is_balanced() {
        // 30% ratio exactly at lower threshold (50% +- 20%)
        let inv = inventory(30, 0, 70, 0);
        let thresh = threshold("0.5", "0.2");

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    fn usdc_venue(available: i64, inflight: i64) -> VenueBalance<Usdc> {
        VenueBalance::new(
            Usdc(Decimal::from(available)),
            Usdc(Decimal::from(inflight)),
        )
    }

    fn usdc_inventory(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> Inventory<Usdc> {
        Inventory {
            onchain: usdc_venue(onchain_available, onchain_inflight),
            offchain: usdc_venue(offchain_available, offchain_inflight),
            last_rebalancing: None,
        }
    }

    fn make_view(equities: Vec<(Symbol, Inventory<FractionalShares>)>) -> InventoryView {
        InventoryView {
            usdc: usdc_inventory(1000, 0, 1000, 0),
            equities: equities.into_iter().collect(),
            last_updated: Utc::now(),
        }
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
            broker_order_id: BrokerOrderId("ORD123".to_string()),
            price_cents: PriceCents(15000),
            broker_timestamp: Utc::now(),
        }
    }

    #[test]
    fn apply_onchain_buy_increases_onchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_onchain_fill(shares(10), Direction::Buy);

        let updated = view
            .apply_position_event(&symbol, &event, Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(110));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
    }

    #[test]
    fn apply_onchain_sell_decreases_onchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_onchain_fill(shares(10), Direction::Sell);

        let updated = view
            .apply_position_event(&symbol, &event, Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(90));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
    }

    #[test]
    fn apply_offchain_buy_increases_offchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_offchain_fill(shares(10), Direction::Buy);

        let updated = view
            .apply_position_event(&symbol, &event, Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(110));
    }

    #[test]
    fn apply_offchain_sell_decreases_offchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_offchain_fill(shares(10), Direction::Sell);

        let updated = view
            .apply_position_event(&symbol, &event, Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(90));
    }

    #[test]
    fn apply_position_event_tracks_symbols_independently() {
        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();
        let view = make_view(vec![
            (aapl.clone(), inventory(100, 0, 100, 0)),
            (msft.clone(), inventory(50, 0, 50, 0)),
        ]);

        let event = make_onchain_fill(shares(10), Direction::Buy);
        let updated = view
            .apply_position_event(&aapl, &event, Utc::now())
            .unwrap();

        let aapl_inv = updated.equities.get(&aapl).unwrap();
        assert_eq!(aapl_inv.onchain.total().unwrap().0, Decimal::from(110));

        let msft_inv = updated.equities.get(&msft).unwrap();
        assert_eq!(msft_inv.onchain.total().unwrap().0, Decimal::from(50));
    }

    #[test]
    fn apply_position_event_unknown_symbol_returns_error() {
        let view = make_view(vec![]);
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_onchain_fill(shares(10), Direction::Buy);

        let result = view.apply_position_event(&symbol, &event, Utc::now());

        assert!(matches!(result, Err(InventoryViewError::UnknownSymbol(_))));
    }

    #[test]
    fn apply_position_event_other_events_only_update_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let original_time = Utc::now();
        let view = InventoryView {
            usdc: usdc_inventory(1000, 0, 1000, 0),
            equities: vec![(symbol.clone(), inventory(100, 0, 100, 0))]
                .into_iter()
                .collect(),
            last_updated: original_time,
        };

        let new_time = original_time + chrono::Duration::hours(1);
        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at: new_time,
        };

        let updated = view
            .apply_position_event(&symbol, &event, new_time)
            .unwrap();

        assert_eq!(updated.last_updated, new_time);

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
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

    fn make_tokens_received(shares_minted: U256) -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted,
            received_at: Utc::now(),
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
            reason: "Token verification failed".to_string(),
            failed_at: Utc::now(),
        }
    }

    #[test]
    fn apply_mint_requested_only_updates_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_mint_requested(&symbol, dec!(50));

        let updated = view
            .apply_mint_event(&symbol, &event, shares(50), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
    }

    #[test]
    fn apply_mint_accepted_moves_offchain_to_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_mint_accepted();

        let updated = view
            .apply_mint_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn apply_tokens_received_transfers_inflight_to_onchain() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Start with 30 shares inflight offchain (simulating post-MintAccepted state)
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 70, 30))]);
        let event = make_tokens_received(U256::from(30_000_000_000_000_000_000_u128));

        let updated = view
            .apply_mint_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(130));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(70));
        assert!(!inv.has_inflight());
    }

    #[test]
    fn apply_mint_completed_updates_last_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(130, 0, 70, 0))]);
        let event = make_mint_completed();

        let updated = view
            .apply_mint_event(&symbol, &event, shares(0), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert!(inv.last_rebalancing.is_some());
    }

    #[test]
    fn apply_mint_rejected_only_updates_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_mint_rejected();

        let updated = view
            .apply_mint_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(!inv.has_inflight());
    }

    #[test]
    fn apply_mint_acceptance_failed_cancels_inflight_back_to_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 70, 30))]);
        let event = make_mint_acceptance_failed();

        let updated = view
            .apply_mint_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(!inv.has_inflight());
    }

    #[test]
    fn apply_token_receipt_failed_keeps_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 70, 30))]);
        let event = make_token_receipt_failed();

        let updated = view
            .apply_mint_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn mint_full_lifecycle_updates_inventory_correctly() {
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = shares(30);

        // Initial state: 100 onchain, 100 offchain
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);

        // MintRequested: No balance change
        let view = view
            .apply_mint_event(
                &symbol,
                &make_mint_requested(&symbol, dec!(30)),
                quantity,
                Utc::now(),
            )
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));

        // MintAccepted: Move 30 from offchain available to inflight
        let view = view
            .apply_mint_event(&symbol, &make_mint_accepted(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());

        // TokensReceived: Remove from offchain inflight, add to onchain available
        let view = view
            .apply_mint_event(
                &symbol,
                &make_tokens_received(U256::from(30_000_000_000_000_000_000_u128)),
                quantity,
                Utc::now(),
            )
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(130));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(70));
        assert!(!inv.has_inflight());

        // MintCompleted: Update last_rebalancing
        let view = view
            .apply_mint_event(&symbol, &make_mint_completed(), shares(0), Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert!(inv.last_rebalancing.is_some());
    }

    #[test]
    fn mint_acceptance_failure_recovery_restores_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = shares(30);

        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);

        let view = view
            .apply_mint_event(
                &symbol,
                &make_mint_requested(&symbol, dec!(30)),
                quantity,
                Utc::now(),
            )
            .unwrap();

        let view = view
            .apply_mint_event(&symbol, &make_mint_accepted(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert!(inv.has_inflight());

        let view = view
            .apply_mint_event(
                &symbol,
                &make_mint_acceptance_failed(),
                quantity,
                Utc::now(),
            )
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(!inv.has_inflight());
    }

    #[test]
    fn mint_token_receipt_failure_keeps_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = shares(30);

        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);

        let view = view
            .apply_mint_event(
                &symbol,
                &make_mint_requested(&symbol, dec!(30)),
                quantity,
                Utc::now(),
            )
            .unwrap();

        let view = view
            .apply_mint_event(&symbol, &make_mint_accepted(), quantity, Utc::now())
            .unwrap();

        let view = view
            .apply_mint_event(
                &symbol,
                &make_tokens_received(U256::from(30_000_000_000_000_000_000_u128)),
                quantity,
                Utc::now(),
            )
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert!(!inv.has_inflight());

        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 70, 30))]);
        let view = view
            .apply_mint_event(&symbol, &make_token_receipt_failed(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert!(inv.has_inflight());
    }

    #[test]
    fn inflight_blocks_imbalance_detection_during_mint() {
        let thresh = threshold("0.5", "0.2");

        // Start with imbalanced inventory: 20% onchain, 80% offchain
        // This should trigger TooMuchOffchain normally
        let inv = inventory(20, 0, 80, 0);
        assert!(matches!(
            inv.detect_imbalance(&thresh),
            Some(Imbalance::TooMuchOffchain { .. })
        ));

        // Now simulate mint in progress: move 30 to inflight
        // Even though still imbalanced, inflight should block detection
        let inv_with_inflight = inventory(20, 0, 50, 30);
        assert!(inv_with_inflight.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn apply_mint_event_unknown_symbol_returns_error() {
        let view = make_view(vec![]);
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_mint_accepted();

        let result = view.apply_mint_event(&symbol, &event, shares(30), Utc::now());

        assert!(matches!(result, Err(InventoryViewError::UnknownSymbol(_))));
    }

    fn make_tokens_sent(symbol: &Symbol, quantity: Decimal) -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokensSent {
            symbol: symbol.clone(),
            quantity,
            redemption_wallet: Address::random(),
            tx_hash: TxHash::random(),
            sent_at: Utc::now(),
        }
    }

    fn make_redemption_detected() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        }
    }

    fn make_redemption_completed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::Completed {
            completed_at: Utc::now(),
        }
    }

    fn make_token_send_failed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::TokenSendFailed {
            reason: "Transaction reverted".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_detection_failed() -> EquityRedemptionEvent {
        EquityRedemptionEvent::DetectionFailed {
            reason: "Alpaca timeout".to_string(),
            failed_at: Utc::now(),
        }
    }

    fn make_redemption_rejected() -> EquityRedemptionEvent {
        EquityRedemptionEvent::RedemptionRejected {
            reason: "Insufficient balance".to_string(),
            rejected_at: Utc::now(),
        }
    }

    #[test]
    fn apply_tokens_sent_moves_onchain_to_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_tokens_sent(&symbol, dec!(30));

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn apply_redemption_detected_only_updates_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Start with 30 shares inflight onchain (simulating post-TokensSent state)
        let view = make_view(vec![(symbol.clone(), inventory(70, 30, 100, 0))]);
        let event = make_redemption_detected();

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn apply_redemption_completed_transfers_inflight_to_offchain() {
        let symbol = Symbol::new("AAPL").unwrap();
        // Start with 30 shares inflight onchain (simulating post-TokensSent state)
        let view = make_view(vec![(symbol.clone(), inventory(70, 30, 100, 0))]);
        let event = make_redemption_completed();

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(70));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(130));
        assert!(!inv.has_inflight());
        assert!(inv.last_rebalancing.is_some());
    }

    #[test]
    fn apply_token_send_failed_keeps_funds_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(70, 30, 100, 0))]);
        let event = make_token_send_failed();

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn apply_detection_failed_keeps_funds_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(70, 30, 100, 0))]);
        let event = make_detection_failed();

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn apply_redemption_rejected_keeps_funds_inflight() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(70, 30, 100, 0))]);
        let event = make_redemption_rejected();

        let updated = view
            .apply_redemption_event(&symbol, &event, shares(30), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());
    }

    #[test]
    fn redemption_full_lifecycle_updates_inventory_correctly() {
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = shares(30);

        // Initial state: 100 onchain, 100 offchain
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);

        // TokensSent: Move 30 from onchain available to inflight
        let view = view
            .apply_redemption_event(
                &symbol,
                &make_tokens_sent(&symbol, dec!(30)),
                quantity,
                Utc::now(),
            )
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());

        // Detected: No balance change
        let view = view
            .apply_redemption_event(&symbol, &make_redemption_detected(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(100));
        assert!(inv.has_inflight());

        // Completed: Remove from onchain inflight, add to offchain available
        let view = view
            .apply_redemption_event(&symbol, &make_redemption_completed(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().0, Decimal::from(70));
        assert_eq!(inv.offchain.total().unwrap().0, Decimal::from(130));
        assert!(!inv.has_inflight());
        assert!(inv.last_rebalancing.is_some());
    }

    #[test]
    fn redemption_rejection_keeps_inflight_and_blocks_rebalancing() {
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = shares(30);
        let thresh = threshold("0.5", "0.2");

        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);

        let view = view
            .apply_redemption_event(
                &symbol,
                &make_tokens_sent(&symbol, dec!(30)),
                quantity,
                Utc::now(),
            )
            .unwrap();

        let view = view
            .apply_redemption_event(&symbol, &make_redemption_detected(), quantity, Utc::now())
            .unwrap();

        let view = view
            .apply_redemption_event(&symbol, &make_redemption_rejected(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert!(inv.has_inflight());

        assert!(inv.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn inflight_blocks_imbalance_detection_during_redemption() {
        let thresh = threshold("0.5", "0.2");

        // Start with imbalanced inventory: 80% onchain, 20% offchain
        // This should trigger TooMuchOnchain normally
        let inv = inventory(80, 0, 20, 0);
        assert!(matches!(
            inv.detect_imbalance(&thresh),
            Some(Imbalance::TooMuchOnchain { .. })
        ));

        // Now simulate redemption in progress: move 30 to onchain inflight
        // Even though still imbalanced, inflight should block detection
        let inv_with_inflight = inventory(50, 30, 20, 0);
        assert!(inv_with_inflight.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn apply_redemption_event_unknown_symbol_returns_error() {
        let view = make_view(vec![]);
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_tokens_sent(&symbol, dec!(30));

        let result = view.apply_redemption_event(&symbol, &event, shares(30), Utc::now());

        assert!(matches!(result, Err(InventoryViewError::UnknownSymbol(_))));
    }
}
