//! Inventory view for tracking cross-venue asset positions.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::{Add, Sub};

use super::snapshot::InventorySnapshotEvent;
use super::venue_balance::{InventoryError, VenueBalance};
use crate::equity_redemption::EquityRedemptionEvent;
use crate::position::PositionEvent;
use crate::shares::{ArithmeticError, FractionalShares, HasZero};
use crate::threshold::Usdc;
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent};
use st0x_execution::{Direction, Symbol};

/// Error type for inventory view operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub(crate) enum InventoryViewError {
    #[error("unknown symbol: {0}")]
    UnknownSymbol(Symbol),
    #[error(transparent)]
    Equity(#[from] InventoryError<FractionalShares>),
    #[error(transparent)]
    Usdc(#[from] InventoryError<Usdc>),
    #[error("failed to convert cash balance cents {0} to USDC")]
    CashBalanceConversion(i64),
}

/// Imbalance requiring rebalancing action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Imbalance<T> {
    /// Too much onchain - triggers movement to offchain.
    TooMuchOnchain { excess: T },
    /// Too much offchain - triggers movement to onchain.
    TooMuchOffchain { excess: T },
}

/// Threshold configuration for imbalance detection.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ImbalanceThreshold {
    /// Target ratio of onchain to total (e.g., 0.5 for 50/50 split).
    pub(crate) target: Decimal,
    /// Deviation from target that triggers rebalancing.
    pub(crate) deviation: Decimal,
}

/// Inventory at a pair of venues (onchain/offchain).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct Inventory<T> {
    onchain: VenueBalance<T>,
    offchain: VenueBalance<T>,
    last_rebalancing: Option<DateTime<Utc>>,
}

/// Impl block with minimal bounds for `has_inflight` - shared by all other impl blocks.
impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + std::fmt::Debug,
{
    fn has_inflight(&self) -> bool {
        self.onchain.has_inflight() || self.offchain.has_inflight()
    }
}

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + std::ops::Mul<Decimal, Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + Into<Decimal>
        + std::fmt::Debug,
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

impl<T: HasZero> Default for Inventory<T> {
    fn default() -> Self {
        Self {
            onchain: VenueBalance::default(),
            offchain: VenueBalance::default(),
            last_rebalancing: None,
        }
    }
}

impl<T> Inventory<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + PartialOrd
        + std::fmt::Debug,
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

    /// Complete a transfer from offchain to onchain, accounting for fees.
    /// Confirms `amount_sent` left offchain, adds `amount_received` to onchain.
    /// The difference is the fee lost in transit (e.g., CCTP bridging fees).
    fn transfer_offchain_to_onchain_with_fee(
        self,
        amount_sent: T,
        amount_received: T,
    ) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            offchain: self.offchain.confirm_inflight(amount_sent)?,
            onchain: self.onchain.add_available(amount_received)?,
            ..self
        })
    }

    /// Complete a transfer from onchain to offchain, accounting for fees.
    /// Confirms `amount_sent` left onchain, adds `amount_received` to offchain.
    /// The difference is the fee lost in transit (e.g., CCTP bridging fees).
    fn transfer_onchain_to_offchain_with_fee(
        self,
        amount_sent: T,
        amount_received: T,
    ) -> Result<Self, InventoryError<T>> {
        Ok(Self {
            onchain: self.onchain.confirm_inflight(amount_sent)?,
            offchain: self.offchain.add_available(amount_received)?,
            ..self
        })
    }

    fn with_last_rebalancing(self, timestamp: DateTime<Utc>) -> Self {
        Self {
            last_rebalancing: Some(timestamp),
            ..self
        }
    }

    /// Apply a fetched onchain venue snapshot.
    /// Skips if ANY venue (onchain or offchain) has inflight operations,
    /// because we cannot distinguish between "transfer completed but not
    /// confirmed" vs "unrelated inventory change".
    fn apply_onchain_snapshot(self, snapshot_balance: T) -> Self {
        if self.has_inflight() {
            return self;
        }

        Self {
            onchain: self.onchain.apply_snapshot(snapshot_balance),
            ..self
        }
    }

    /// Apply a fetched offchain venue snapshot.
    /// Skips if ANY venue (onchain or offchain) has inflight operations,
    /// because we cannot distinguish between "transfer completed but not
    /// confirmed" vs "unrelated inventory change".
    fn apply_offchain_snapshot(self, snapshot_balance: T) -> Self {
        if self.has_inflight() {
            return self;
        }

        Self {
            offchain: self.offchain.apply_snapshot(snapshot_balance),
            ..self
        }
    }
}

/// Cross-aggregate projection tracking inventory across venues.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct InventoryView {
    usdc: Inventory<Usdc>,
    equities: HashMap<Symbol, Inventory<FractionalShares>>,
    last_updated: DateTime<Utc>,
}

impl InventoryView {
    /// Checks a single equity for imbalance against the threshold.
    /// Returns the imbalance if one exists, or None if balanced or symbol not tracked.
    pub(crate) fn check_equity_imbalance(
        &self,
        symbol: &Symbol,
        threshold: &ImbalanceThreshold,
    ) -> Option<Imbalance<FractionalShares>> {
        let inventory = self.equities.get(symbol)?;
        inventory.detect_imbalance(threshold)
    }

    /// Checks USDC inventory for imbalance against the threshold.
    /// Returns the imbalance if one exists.
    pub(crate) fn check_usdc_imbalance(
        &self,
        threshold: &ImbalanceThreshold,
    ) -> Option<Imbalance<Usdc>> {
        self.usdc.detect_imbalance(threshold)
    }
}

impl Default for InventoryView {
    fn default() -> Self {
        Self {
            usdc: Inventory::default(),
            equities: HashMap::new(),
            last_updated: Utc::now(),
        }
    }
}

impl InventoryView {
    /// Registers a symbol with zeroed inventory.
    #[cfg(test)]
    pub(crate) fn with_equity(mut self, symbol: Symbol) -> Self {
        self.equities.insert(symbol, Inventory::default());
        self
    }

    /// Sets USDC inventory with specified available balances (zero inflight).
    #[cfg(test)]
    pub(crate) fn with_usdc(self, onchain_available: Usdc, offchain_available: Usdc) -> Self {
        Self {
            usdc: Inventory {
                onchain: VenueBalance::new(onchain_available, Usdc(Decimal::ZERO)),
                offchain: VenueBalance::new(offchain_available, Usdc(Decimal::ZERO)),
                last_rebalancing: None,
            },
            ..self
        }
    }

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

    fn update_usdc(
        self,
        f: impl FnOnce(Inventory<Usdc>) -> Result<Inventory<Usdc>, InventoryError<Usdc>>,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        let updated = f(self.usdc)?;

        Ok(Self {
            usdc: updated,
            last_updated: now,
            equities: self.equities,
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
    ) -> Result<Self, InventoryViewError> {
        let timestamp = event.timestamp();

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
                    timestamp,
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
                    timestamp,
                )
            }

            PositionEvent::Initialized { .. }
            | PositionEvent::Migrated { .. }
            | PositionEvent::OffChainOrderPlaced { .. }
            | PositionEvent::OffChainOrderFailed { .. }
            | PositionEvent::ThresholdUpdated { .. } => Ok(Self {
                last_updated: timestamp,
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
            | TokenizedEquityMintEvent::MintRejected { .. } => Ok(Self {
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

    /// Applies a USDC rebalance event to update USDC inventory.
    ///
    /// - `Initiated`: Move amount from source venue's available to inflight.
    ///   - `AlpacaToBase`: offchain → onchain (move offchain to inflight)
    ///   - `BaseToAlpaca`: onchain → offchain (move onchain to inflight)
    /// - `WithdrawalConfirmed`: No balance change (awaiting bridge).
    /// - `WithdrawalFailed`: Keep inflight until manually resolved.
    /// - `BridgingInitiated`, `BridgeAttestationReceived`: No balance change.
    /// - `Bridged`: Remove from source inflight, add to destination available.
    /// - `BridgingFailed`: Keep inflight until manually resolved.
    /// - `DepositInitiated`: No balance change.
    /// - `DepositConfirmed`: Update `last_rebalancing` timestamp.
    /// - `DepositFailed`: Keep inflight until manually resolved.
    ///
    /// The `amount` parameter is the rebalance amount, needed for events that
    /// don't carry the amount themselves.
    pub(crate) fn apply_usdc_rebalance_event(
        self,
        event: &UsdcRebalanceEvent,
        direction: &RebalanceDirection,
        amount: Usdc,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        match (event, direction) {
            (UsdcRebalanceEvent::Initiated { .. }, RebalanceDirection::AlpacaToBase) => {
                self.update_usdc(|inv| inv.move_offchain_to_inflight(amount), now)
            }
            (UsdcRebalanceEvent::Initiated { .. }, RebalanceDirection::BaseToAlpaca) => {
                self.update_usdc(|inv| inv.move_onchain_to_inflight(amount), now)
            }

            (
                UsdcRebalanceEvent::Bridged {
                    amount_received, ..
                },
                RebalanceDirection::AlpacaToBase,
            ) => self.update_usdc(
                |inv| inv.transfer_offchain_to_onchain_with_fee(amount, *amount_received),
                now,
            ),
            (
                UsdcRebalanceEvent::Bridged {
                    amount_received, ..
                },
                RebalanceDirection::BaseToAlpaca,
            ) => self.update_usdc(
                |inv| inv.transfer_onchain_to_offchain_with_fee(amount, *amount_received),
                now,
            ),

            (
                UsdcRebalanceEvent::DepositConfirmed {
                    deposit_confirmed_at,
                    ..
                },
                _,
            ) => self.update_usdc(
                |inv| Ok(inv.with_last_rebalancing(*deposit_confirmed_at)),
                now,
            ),

            // ConversionConfirmed affects offchain USDC:
            // - AlpacaToBase (USD->USDC): Adds USDC to offchain (arrived in crypto wallet)
            // - BaseToAlpaca (USDC->USD): Removes USDC from offchain (converted to USD)
            (
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount,
                    ..
                },
                _,
            ) => self.update_usdc(|inv| inv.add_offchain_available(*filled_amount), now),

            (
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    filled_amount,
                    ..
                },
                _,
            ) => self.update_usdc(|inv| inv.remove_offchain_available(*filled_amount), now),

            (
                UsdcRebalanceEvent::ConversionInitiated { .. }
                | UsdcRebalanceEvent::ConversionFailed { .. }
                | UsdcRebalanceEvent::WithdrawalConfirmed { .. }
                | UsdcRebalanceEvent::WithdrawalFailed { .. }
                | UsdcRebalanceEvent::BridgingInitiated { .. }
                | UsdcRebalanceEvent::BridgeAttestationReceived { .. }
                | UsdcRebalanceEvent::BridgingFailed { .. }
                | UsdcRebalanceEvent::DepositInitiated { .. }
                | UsdcRebalanceEvent::DepositFailed { .. },
                _,
            ) => Ok(Self {
                last_updated: now,
                ..self
            }),
        }
    }

    /// Applies an inventory snapshot event to reconcile tracked inventory with fetched actuals.
    ///
    /// For each symbol/venue combination, sets `available = max(0, actual - inflight)` to
    /// match the fetched total while preserving inflight operations.
    ///
    /// - `OnchainEquity`: Reconcile onchain equity balances for all fetched symbols.
    /// - `OnchainCash`: Reconcile onchain USDC balance.
    /// - `OffchainEquity`: Reconcile offchain equity positions for all fetched symbols.
    /// - `OffchainCash`: Reconcile offchain cash balance (converted from cents to Usdc).
    pub(crate) fn apply_snapshot_event(
        self,
        event: &InventorySnapshotEvent,
        now: DateTime<Utc>,
    ) -> Result<Self, InventoryViewError> {
        match event {
            InventorySnapshotEvent::OnchainEquity { balances, .. } => {
                balances
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            |inventory| Ok(inventory.apply_onchain_snapshot(*snapshot_balance)),
                            now,
                        )
                    })
            }

            InventorySnapshotEvent::OnchainCash { usdc_balance, .. } => self.update_usdc(
                |inventory| Ok(inventory.apply_onchain_snapshot(*usdc_balance)),
                now,
            ),

            InventorySnapshotEvent::OffchainEquity { positions, .. } => {
                positions
                    .iter()
                    .try_fold(self, |view, (symbol, snapshot_balance)| {
                        view.update_equity(
                            symbol,
                            |inventory| Ok(inventory.apply_offchain_snapshot(*snapshot_balance)),
                            now,
                        )
                    })
            }

            InventorySnapshotEvent::OffchainCash {
                cash_balance_cents, ..
            } => {
                let usdc = Usdc::from_cents(*cash_balance_cents).ok_or(
                    InventoryViewError::CashBalanceConversion(*cash_balance_cents),
                )?;
                self.update_usdc(|inventory| Ok(inventory.apply_offchain_snapshot(usdc)), now)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use alloy::primitives::{Address, TxHash, U256};
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::inventory::snapshot::InventorySnapshotEvent;
    use crate::offchain_order::{BrokerOrderId, ExecutionId, PriceCents};
    use crate::position::TradeId;
    use crate::threshold::ExecutionThreshold;
    use crate::tokenized_equity_mint::{IssuerRequestId, ReceiptId, TokenizationRequestId};

    fn shares(n: i64) -> FractionalShares {
        FractionalShares::new(Decimal::from(n))
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

        let updated = view.apply_position_event(&symbol, &event).unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(110));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
    }

    #[test]
    fn apply_onchain_sell_decreases_onchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_onchain_fill(shares(10), Direction::Sell);

        let updated = view.apply_position_event(&symbol, &event).unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(90));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
    }

    #[test]
    fn apply_offchain_buy_increases_offchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_offchain_fill(shares(10), Direction::Buy);

        let updated = view.apply_position_event(&symbol, &event).unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(110));
    }

    #[test]
    fn apply_offchain_sell_decreases_offchain_available() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_offchain_fill(shares(10), Direction::Sell);

        let updated = view.apply_position_event(&symbol, &event).unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(90));
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
        let updated = view.apply_position_event(&aapl, &event).unwrap();

        let aapl_inv = updated.equities.get(&aapl).unwrap();
        assert_eq!(
            aapl_inv.onchain.total().unwrap().inner(),
            Decimal::from(110)
        );

        let msft_inv = updated.equities.get(&msft).unwrap();
        assert_eq!(msft_inv.onchain.total().unwrap().inner(), Decimal::from(50));
    }

    #[test]
    fn apply_position_event_unknown_symbol_returns_error() {
        let view = make_view(vec![]);
        let symbol = Symbol::new("AAPL").unwrap();
        let event = make_onchain_fill(shares(10), Direction::Buy);

        let result = view.apply_position_event(&symbol, &event);

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

        let event_time = original_time + chrono::Duration::hours(1);
        let event = PositionEvent::Initialized {
            symbol: symbol.clone(),
            threshold: ExecutionThreshold::whole_share(),
            initialized_at: event_time,
        };

        let updated = view.apply_position_event(&symbol, &event).unwrap();

        // Timestamp should come from the event, not Utc::now()
        assert_eq!(updated.last_updated, event_time);

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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

    #[test]
    fn apply_mint_requested_only_updates_last_updated() {
        let symbol = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(symbol.clone(), inventory(100, 0, 100, 0))]);
        let event = make_mint_requested(&symbol, dec!(50));

        let updated = view
            .apply_mint_event(&symbol, &event, shares(50), Utc::now())
            .unwrap();

        let inv = updated.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(130));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(70));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
        assert!(!inv.has_inflight());
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));

        // MintAccepted: Move 30 from offchain available to inflight
        let view = view
            .apply_mint_event(&symbol, &make_mint_accepted(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(130));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(70));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(70));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(130));
        assert!(!inv.has_inflight());
        assert!(inv.last_rebalancing.is_some());
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(100));
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
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert!(inv.has_inflight());

        // Detected: No balance change
        let view = view
            .apply_redemption_event(&symbol, &make_redemption_detected(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(100));
        assert!(inv.has_inflight());

        // Completed: Remove from onchain inflight, add to offchain available
        let view = view
            .apply_redemption_event(&symbol, &make_redemption_completed(), quantity, Utc::now())
            .unwrap();
        let inv = view.equities.get(&symbol).unwrap();
        assert_eq!(inv.onchain.total().unwrap().inner(), Decimal::from(70));
        assert_eq!(inv.offchain.total().unwrap().inner(), Decimal::from(130));
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

    fn usdc(n: i64) -> Usdc {
        Usdc(Decimal::from(n))
    }

    fn make_usdc_view(
        onchain_available: i64,
        onchain_inflight: i64,
        offchain_available: i64,
        offchain_inflight: i64,
    ) -> InventoryView {
        InventoryView {
            usdc: usdc_inventory(
                onchain_available,
                onchain_inflight,
                offchain_available,
                offchain_inflight,
            ),
            equities: HashMap::new(),
            last_updated: Utc::now(),
        }
    }

    fn make_initiated_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: usdc(100),
            withdrawal_ref: crate::usdc_rebalance::TransferRef::OnchainTx(TxHash::random()),
            initiated_at: Utc::now(),
        }
    }

    fn make_bridged_event(amount_received: Usdc, fee_collected: Usdc) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Bridged {
            mint_tx_hash: TxHash::random(),
            amount_received,
            fee_collected,
            minted_at: Utc::now(),
        }
    }

    fn make_deposit_confirmed_event(direction: RebalanceDirection) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositConfirmed {
            direction,
            deposit_confirmed_at: Utc::now(),
        }
    }

    #[test]
    fn apply_usdc_initiated_alpaca_to_base_moves_offchain_to_inflight() {
        let view = make_usdc_view(1000, 0, 1000, 0);
        let event = make_initiated_event();

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                usdc(100),
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            updated.usdc.offchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert_eq!(
            updated.usdc.onchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert!(updated.usdc.offchain.has_inflight());
        assert!(!updated.usdc.onchain.has_inflight());
    }

    #[test]
    fn apply_usdc_initiated_base_to_alpaca_moves_onchain_to_inflight() {
        let view = make_usdc_view(1000, 0, 1000, 0);
        let event = make_initiated_event();

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::BaseToAlpaca,
                usdc(100),
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            updated.usdc.onchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert_eq!(
            updated.usdc.offchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert!(updated.usdc.onchain.has_inflight());
        assert!(!updated.usdc.offchain.has_inflight());
    }

    #[test]
    fn apply_usdc_bridged_alpaca_to_base_transfers_to_onchain() {
        let view = make_usdc_view(1000, 0, 900, 100);
        let event = make_bridged_event(usdc(100), Usdc(dec!(0)));

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                usdc(100),
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            updated.usdc.onchain.total().unwrap().inner(),
            Decimal::from(1100)
        );
        assert_eq!(
            updated.usdc.offchain.total().unwrap().inner(),
            Decimal::from(900)
        );
        assert!(!updated.usdc.offchain.has_inflight());
    }

    #[test]
    fn apply_usdc_bridged_base_to_alpaca_transfers_to_offchain() {
        let view = make_usdc_view(900, 100, 1000, 0);
        let event = make_bridged_event(usdc(100), Usdc(dec!(0)));

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::BaseToAlpaca,
                usdc(100),
                Utc::now(),
            )
            .unwrap();

        assert_eq!(
            updated.usdc.offchain.total().unwrap().inner(),
            Decimal::from(1100)
        );
        assert_eq!(
            updated.usdc.onchain.total().unwrap().inner(),
            Decimal::from(900)
        );
        assert!(!updated.usdc.onchain.has_inflight());
    }

    #[test]
    fn apply_usdc_deposit_confirmed_updates_last_rebalancing() {
        let view = make_usdc_view(1100, 0, 900, 0);
        let direction = RebalanceDirection::AlpacaToBase;
        let event = make_deposit_confirmed_event(direction.clone());

        let updated = view
            .apply_usdc_rebalance_event(&event, &direction, usdc(100), Utc::now())
            .unwrap();

        assert!(updated.usdc.last_rebalancing.is_some());
    }

    #[test]
    fn usdc_alpaca_to_base_full_lifecycle() {
        let view = make_usdc_view(1000, 0, 1000, 0);
        let amount = usdc(200);
        let direction = RebalanceDirection::AlpacaToBase;

        let after_initiated = view
            .apply_usdc_rebalance_event(&make_initiated_event(), &direction, amount, Utc::now())
            .unwrap();
        assert_eq!(
            after_initiated.usdc.offchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert!(after_initiated.usdc.has_inflight());

        let after_bridged = after_initiated
            .apply_usdc_rebalance_event(
                &make_bridged_event(amount, Usdc(dec!(0))),
                &direction,
                amount,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(
            after_bridged.usdc.onchain.total().unwrap().inner(),
            Decimal::from(1200)
        );
        assert_eq!(
            after_bridged.usdc.offchain.total().unwrap().inner(),
            Decimal::from(800)
        );
        assert!(!after_bridged.usdc.has_inflight());

        let after_confirmed = after_bridged
            .apply_usdc_rebalance_event(
                &make_deposit_confirmed_event(direction.clone()),
                &direction,
                amount,
                Utc::now(),
            )
            .unwrap();
        assert!(after_confirmed.usdc.last_rebalancing.is_some());
    }

    #[test]
    fn usdc_base_to_alpaca_full_lifecycle() {
        let view = make_usdc_view(1000, 0, 1000, 0);
        let amount = usdc(200);
        let direction = RebalanceDirection::BaseToAlpaca;

        let after_initiated = view
            .apply_usdc_rebalance_event(&make_initiated_event(), &direction, amount, Utc::now())
            .unwrap();
        assert_eq!(
            after_initiated.usdc.onchain.total().unwrap().inner(),
            Decimal::from(1000)
        );
        assert!(after_initiated.usdc.has_inflight());

        let after_bridged = after_initiated
            .apply_usdc_rebalance_event(
                &make_bridged_event(amount, Usdc(dec!(0))),
                &direction,
                amount,
                Utc::now(),
            )
            .unwrap();
        assert_eq!(
            after_bridged.usdc.offchain.total().unwrap().inner(),
            Decimal::from(1200)
        );
        assert_eq!(
            after_bridged.usdc.onchain.total().unwrap().inner(),
            Decimal::from(800)
        );
        assert!(!after_bridged.usdc.has_inflight());

        let after_confirmed = after_bridged
            .apply_usdc_rebalance_event(
                &make_deposit_confirmed_event(direction.clone()),
                &direction,
                amount,
                Utc::now(),
            )
            .unwrap();
        assert!(after_confirmed.usdc.last_rebalancing.is_some());
    }

    #[test]
    fn usdc_failure_events_keep_inflight() {
        let view = make_usdc_view(1000, 0, 800, 200);
        let amount = usdc(200);
        let direction = RebalanceDirection::AlpacaToBase;

        let withdrawal_failed = UsdcRebalanceEvent::WithdrawalFailed {
            reason: "timeout".to_string(),
            failed_at: Utc::now(),
        };
        let after_withdrawal_failed = view
            .clone()
            .apply_usdc_rebalance_event(&withdrawal_failed, &direction, amount, Utc::now())
            .unwrap();
        assert!(after_withdrawal_failed.usdc.offchain.has_inflight());

        let bridging_failed = UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: Some(TxHash::random()),
            cctp_nonce: Some(123),
            reason: "attestation timeout".to_string(),
            failed_at: Utc::now(),
        };
        let after_bridging_failed = view
            .clone()
            .apply_usdc_rebalance_event(&bridging_failed, &direction, amount, Utc::now())
            .unwrap();
        assert!(after_bridging_failed.usdc.offchain.has_inflight());

        let deposit_failed = UsdcRebalanceEvent::DepositFailed {
            deposit_ref: None,
            reason: "rejected".to_string(),
            failed_at: Utc::now(),
        };
        let after_deposit_failed = view
            .apply_usdc_rebalance_event(&deposit_failed, &direction, amount, Utc::now())
            .unwrap();
        assert!(after_deposit_failed.usdc.offchain.has_inflight());
    }

    #[test]
    fn usdc_inflight_blocks_imbalance_detection() {
        let inv = usdc_inventory(800, 0, 200, 0);
        let thresh = threshold("0.5", "0.2");
        assert!(inv.detect_imbalance(&thresh).is_some());

        let inv_with_inflight = usdc_inventory(700, 100, 200, 0);
        assert!(inv_with_inflight.detect_imbalance(&thresh).is_none());
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_balanced() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), inventory(50, 0, 50, 0))]);
        let thresh = threshold("0.5", "0.2");

        assert!(view.check_equity_imbalance(&aapl, &thresh).is_none());
    }

    #[test]
    fn check_equity_imbalance_detects_too_much_onchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");

        let imbalance = view.check_equity_imbalance(&aapl, &thresh);

        assert!(matches!(imbalance, Some(Imbalance::TooMuchOnchain { .. })));
    }

    #[test]
    fn check_equity_imbalance_detects_too_much_offchain() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), inventory(20, 0, 80, 0))]);
        let thresh = threshold("0.5", "0.2");

        let imbalance = view.check_equity_imbalance(&aapl, &thresh);

        assert!(matches!(imbalance, Some(Imbalance::TooMuchOffchain { .. })));
    }

    #[test]
    fn check_equity_imbalance_returns_none_for_unknown_symbol() {
        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();
        let view = make_view(vec![(aapl, inventory(80, 0, 20, 0))]);
        let thresh = threshold("0.5", "0.2");

        assert!(view.check_equity_imbalance(&msft, &thresh).is_none());
    }

    #[test]
    fn check_equity_imbalance_returns_none_when_inflight() {
        let aapl = Symbol::new("AAPL").unwrap();
        let view = make_view(vec![(aapl.clone(), inventory(60, 20, 20, 0))]);
        let thresh = threshold("0.5", "0.2");

        assert!(view.check_equity_imbalance(&aapl, &thresh).is_none());
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_balanced() {
        let view = make_usdc_view(500, 0, 500, 0);

        assert!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3"))
                .is_none()
        );
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_onchain() {
        let view = make_usdc_view(900, 0, 100, 0);

        let imbalance = view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOnchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_too_much_offchain() {
        let view = make_usdc_view(100, 0, 900, 0);

        let imbalance = view.check_usdc_imbalance(&threshold("0.5", "0.3")).unwrap();

        assert!(matches!(imbalance, Imbalance::TooMuchOffchain { .. }));
    }

    #[test]
    fn check_usdc_imbalance_returns_none_when_inflight() {
        let view = make_usdc_view(700, 200, 100, 0);

        assert!(
            view.check_usdc_imbalance(&threshold("0.5", "0.3"))
                .is_none()
        );
    }

    #[test]
    fn apply_conversion_initiated_updates_timestamp_only() {
        let now = Utc::now();
        let before = now - chrono::Duration::hours(1);

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: before,
        };

        let event = UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(500)),
            order_id: uuid::Uuid::new_v4(),
            initiated_at: now,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                Usdc(dec!(500)),
                now,
            )
            .unwrap();

        // Timestamp should be updated
        assert_eq!(updated.last_updated, now);

        // USDC balances should NOT change for conversion events
        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(1000)));
        assert_eq!(updated.usdc.offchain.available(), Usdc(dec!(1000)));
        assert_eq!(updated.usdc.onchain.inflight(), Usdc(dec!(0)));
        assert_eq!(updated.usdc.offchain.inflight(), Usdc(dec!(0)));
    }

    #[test]
    fn apply_conversion_failed_keeps_balances_unchanged() {
        // ConversionFailed should not change any balances - it just updates timestamp.
        // The conversion order failed, so no USDC was added/removed.
        let now = Utc::now();
        let before = now - chrono::Duration::hours(1);

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: before,
        };

        let event = UsdcRebalanceEvent::ConversionFailed {
            reason: "Order rejected".to_string(),
            failed_at: now,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::BaseToAlpaca,
                Usdc(dec!(500)),
                now,
            )
            .unwrap();

        // Timestamp should be updated
        assert_eq!(updated.last_updated, now);

        // USDC balances should NOT change for failed conversion
        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(1000)));
        assert_eq!(updated.usdc.offchain.available(), Usdc(dec!(1000)));
    }

    #[test]
    fn apply_usdc_bridged_uses_amount_received_not_requested_amount() {
        // BUG TEST: When CCTP fees are deducted, inventory should reflect actual received amount
        // Request: 100 USDC, Fee: 0.01 USDC, Actual received: 99.99 USDC
        let requested_amount = usdc(100);
        let amount_received = Usdc(dec!(99.99));
        let fee_collected = Usdc(dec!(0.01));

        // Start with 100 inflight (the requested amount)
        let view = make_usdc_view(1000, 0, 900, 100);

        // The Bridged event should contain the actual amount received
        let event = UsdcRebalanceEvent::Bridged {
            mint_tx_hash: TxHash::random(),
            minted_at: Utc::now(),
            amount_received,
            fee_collected,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                requested_amount, // This should be ignored - event's amount_received should be used
                Utc::now(),
            )
            .unwrap();

        // Onchain should receive the ACTUAL amount (99.99), not the requested (100)
        assert_eq!(
            updated.usdc.onchain.total().unwrap().inner(),
            dec!(1099.99),
            "onchain should have 1000 + 99.99 (actual received), not 1000 + 100 (requested)"
        );

        // Offchain should have the fee deducted from total
        // Started with 900 available + 100 inflight = 1000 total
        // After bridge: 100 inflight consumed, but only 99.99 arrived at destination
        // So offchain total is now 900 (the 0.01 fee was lost in transit)
        assert_eq!(
            updated.usdc.offchain.total().unwrap().inner(),
            dec!(900),
            "offchain should have 900 (inflight consumed)"
        );
    }

    #[test]
    fn conversion_events_do_not_affect_usdc_inflight() {
        let now = Utc::now();

        // Start with some inflight (simulating mid-rebalance)
        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(400)), Usdc(dec!(100))), // 100 inflight
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        // Conversion events should not modify inflight amounts
        let event = UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(100)),
            order_id: uuid::Uuid::new_v4(),
            initiated_at: now,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                Usdc(dec!(100)),
                now,
            )
            .unwrap();

        // Inflight should remain unchanged - conversion is a separate concern from bridging
        assert_eq!(updated.usdc.offchain.inflight(), Usdc(dec!(100)));
        assert_eq!(updated.usdc.offchain.available(), Usdc(dec!(400)));
    }

    #[test]
    fn conversion_confirmed_base_to_alpaca_removes_usdc_from_offchain() {
        // BUG TEST: For BaseToAlpaca flow, ConversionConfirmed (USDC->USD) should
        // remove USDC from offchain. Currently, it just updates the timestamp.
        //
        // Scenario: After BaseToAlpaca deposit, we have 1000 USDC in offchain (Alpaca wallet).
        // We convert 100 USDC to USD. Due to slippage (~17 bps), the filled amount
        // might be slightly different, but for USDC->USD we're selling USDC so
        // filled_amount represents the USDC sold (should be same as requested for full fills).
        //
        // After conversion, offchain should have 900 USDC (1000 - 100).
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        // Request: 100 USDC → USD conversion
        // Filled: 100 USDC sold (full fill for market order)
        let filled_amount = Usdc(dec!(100));

        let event = UsdcRebalanceEvent::ConversionConfirmed {
            direction: RebalanceDirection::BaseToAlpaca,
            filled_amount,
            converted_at: now,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::BaseToAlpaca,
                Usdc(dec!(100)),
                now,
            )
            .unwrap();

        // Offchain should have 900 USDC (1000 - 100)
        assert_eq!(
            updated.usdc.offchain.available(),
            Usdc(dec!(900)),
            "ConversionConfirmed(BaseToAlpaca) should remove filled_amount USDC from offchain"
        );

        // Onchain unchanged
        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(500)));
    }

    #[test]
    fn conversion_confirmed_alpaca_to_base_adds_usdc_to_offchain() {
        // BUG TEST: For AlpacaToBase flow, ConversionConfirmed (USD->USDC) should
        // add USDC to offchain. Currently, it just updates the timestamp.
        //
        // Scenario: We're converting USD to USDC before withdrawal.
        // Request: 1000 USD worth of USDC
        // Filled: 998.3 USDC (17 bps slippage - we got less USDC than expected)
        //
        // After conversion, offchain should increase by the filled_amount (998.3), not the requested.
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(1000)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        // Request: 1000 USD → USDC conversion
        // Filled: 998.3 USDC (slippage of ~17 bps)
        let filled_amount = Usdc(dec!(998.3));

        let event = UsdcRebalanceEvent::ConversionConfirmed {
            direction: RebalanceDirection::AlpacaToBase,
            filled_amount,
            converted_at: now,
        };

        let updated = view
            .apply_usdc_rebalance_event(
                &event,
                &RebalanceDirection::AlpacaToBase,
                Usdc(dec!(1000)),
                now,
            )
            .unwrap();

        // Offchain should have 1998.3 USDC (1000 + 998.3)
        assert_eq!(
            updated.usdc.offchain.available(),
            Usdc(dec!(1998.3)),
            "ConversionConfirmed(AlpacaToBase) should add filled_amount USDC to offchain"
        );

        // Onchain unchanged
        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(500)));
    }

    #[test]
    fn snapshot_onchain_equity_skips_when_inflight_nonzero() {
        let now = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();

        let view = InventoryView {
            usdc: Inventory::default(),
            equities: HashMap::from([(
                aapl.clone(),
                Inventory {
                    onchain: VenueBalance::new(shares(90), shares(10)),
                    offchain: VenueBalance::new(shares(50), shares(0)),
                    last_rebalancing: None,
                },
            )]),
            last_updated: now,
        };

        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), shares(95));

        let event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        let equity = updated.equities.get(&aapl).unwrap();
        assert_eq!(
            equity.onchain.available(),
            shares(90),
            "should be unchanged"
        );
        assert_eq!(equity.onchain.inflight(), shares(10), "should be unchanged");
        assert_eq!(
            equity.offchain.available(),
            shares(50),
            "should be unchanged"
        );
    }

    #[test]
    fn snapshot_onchain_equity_reconciles_when_inflight_zero() {
        let now = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();

        let view = InventoryView {
            usdc: Inventory::default(),
            equities: HashMap::from([(
                aapl.clone(),
                Inventory {
                    onchain: VenueBalance::new(shares(90), shares(0)),
                    offchain: VenueBalance::new(shares(50), shares(0)),
                    last_rebalancing: None,
                },
            )]),
            last_updated: now,
        };

        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), shares(95));

        let event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        let equity = updated.equities.get(&aapl).unwrap();
        assert_eq!(equity.onchain.available(), shares(95));
        assert_eq!(equity.onchain.inflight(), shares(0));
        assert_eq!(equity.offchain.available(), shares(50));
    }

    #[test]
    fn snapshot_onchain_cash_skips_when_inflight_nonzero() {
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(900)), Usdc(dec!(100))),
                offchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        let event = InventorySnapshotEvent::OnchainCash {
            usdc_balance: Usdc(dec!(950)),
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        assert_eq!(
            updated.usdc.onchain.available(),
            Usdc(dec!(900)),
            "should be unchanged"
        );
        assert_eq!(
            updated.usdc.onchain.inflight(),
            Usdc(dec!(100)),
            "should be unchanged"
        );
        assert_eq!(
            updated.usdc.offchain.available(),
            Usdc(dec!(500)),
            "should be unchanged"
        );
    }

    #[test]
    fn snapshot_onchain_cash_reconciles_when_inflight_zero() {
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(900)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        let event = InventorySnapshotEvent::OnchainCash {
            usdc_balance: Usdc(dec!(950)),
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(950)));
        assert_eq!(updated.usdc.onchain.inflight(), Usdc(dec!(0)));
        assert_eq!(updated.usdc.offchain.available(), Usdc(dec!(500)));
    }

    #[test]
    fn snapshot_offchain_equity_skips_when_inflight_nonzero() {
        let now = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();

        let view = InventoryView {
            usdc: Inventory::default(),
            equities: HashMap::from([(
                aapl.clone(),
                Inventory {
                    onchain: VenueBalance::new(shares(100), shares(0)),
                    offchain: VenueBalance::new(shares(40), shares(10)),
                    last_rebalancing: None,
                },
            )]),
            last_updated: now,
        };

        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), shares(55));

        let event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        let equity = updated.equities.get(&aapl).unwrap();
        assert_eq!(
            equity.offchain.available(),
            shares(40),
            "should be unchanged"
        );
        assert_eq!(
            equity.offchain.inflight(),
            shares(10),
            "should be unchanged"
        );
        assert_eq!(
            equity.onchain.available(),
            shares(100),
            "should be unchanged"
        );
    }

    #[test]
    fn snapshot_offchain_equity_reconciles_when_inflight_zero() {
        let now = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();

        let view = InventoryView {
            usdc: Inventory::default(),
            equities: HashMap::from([(
                aapl.clone(),
                Inventory {
                    onchain: VenueBalance::new(shares(100), shares(0)),
                    offchain: VenueBalance::new(shares(40), shares(0)),
                    last_rebalancing: None,
                },
            )]),
            last_updated: now,
        };

        let mut positions = BTreeMap::new();
        positions.insert(aapl.clone(), shares(55));

        let event = InventorySnapshotEvent::OffchainEquity {
            positions,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        let equity = updated.equities.get(&aapl).unwrap();
        assert_eq!(equity.offchain.available(), shares(55));
        assert_eq!(equity.offchain.inflight(), shares(0));
        assert_eq!(equity.onchain.available(), shares(100));
    }

    #[test]
    fn snapshot_offchain_cash_skips_when_inflight_nonzero() {
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(900)), Usdc(dec!(100))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        let event = InventorySnapshotEvent::OffchainCash {
            cash_balance_cents: 95000,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        assert_eq!(
            updated.usdc.offchain.available(),
            Usdc(dec!(900)),
            "should be unchanged"
        );
        assert_eq!(
            updated.usdc.offchain.inflight(),
            Usdc(dec!(100)),
            "should be unchanged"
        );
        assert_eq!(
            updated.usdc.onchain.available(),
            Usdc(dec!(500)),
            "should be unchanged"
        );
    }

    #[test]
    fn snapshot_offchain_cash_reconciles_when_inflight_zero() {
        let now = Utc::now();

        let view = InventoryView {
            usdc: Inventory {
                onchain: VenueBalance::new(Usdc(dec!(500)), Usdc(dec!(0))),
                offchain: VenueBalance::new(Usdc(dec!(900)), Usdc(dec!(0))),
                last_rebalancing: None,
            },
            equities: HashMap::new(),
            last_updated: now,
        };

        // 95000 cents = $950.00
        let event = InventorySnapshotEvent::OffchainCash {
            cash_balance_cents: 95000,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        assert_eq!(updated.usdc.offchain.available(), Usdc(dec!(950)));
        assert_eq!(updated.usdc.offchain.inflight(), Usdc(dec!(0)));
        assert_eq!(updated.usdc.onchain.available(), Usdc(dec!(500)));
    }

    #[test]
    fn snapshot_multiple_symbols_reconciled_in_single_event() {
        let now = Utc::now();
        let aapl = Symbol::new("AAPL").unwrap();
        let msft = Symbol::new("MSFT").unwrap();

        let view = InventoryView {
            usdc: Inventory::default(),
            equities: HashMap::from([
                (
                    aapl.clone(),
                    Inventory {
                        onchain: VenueBalance::new(shares(100), shares(0)),
                        offchain: VenueBalance::new(shares(50), shares(0)),
                        last_rebalancing: None,
                    },
                ),
                (
                    msft.clone(),
                    Inventory {
                        onchain: VenueBalance::new(shares(200), shares(0)),
                        offchain: VenueBalance::new(shares(75), shares(0)),
                        last_rebalancing: None,
                    },
                ),
            ]),
            last_updated: now,
        };

        let mut balances = BTreeMap::new();
        balances.insert(aapl.clone(), shares(80));
        balances.insert(msft.clone(), shares(180));

        let event = InventorySnapshotEvent::OnchainEquity {
            balances,
            fetched_at: now,
        };

        let updated = view.apply_snapshot_event(&event, now).unwrap();

        assert_eq!(
            updated.equities.get(&aapl).unwrap().onchain.available(),
            shares(80)
        );
        assert_eq!(
            updated.equities.get(&msft).unwrap().onchain.available(),
            shares(180)
        );
    }
}
