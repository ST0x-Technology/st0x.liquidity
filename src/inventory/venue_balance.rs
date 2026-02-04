//! Generic venue balance tracking for inventory management.

use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use tracing::debug;

use st0x_execution::{ArithmeticError, HasZero};

impl<T: HasZero> Default for VenueBalance<T> {
    fn default() -> Self {
        Self {
            available: T::ZERO,
            inflight: T::ZERO,
        }
    }
}

/// Error type for inventory operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub(crate) enum InventoryError<T> {
    #[error(
        "insufficient available balance: requested {requested:?}, but only {available:?} available"
    )]
    InsufficientAvailable { requested: T, available: T },
    #[error(
        "insufficient inflight balance: requested {requested:?}, but only {inflight:?} inflight"
    )]
    InsufficientInflight { requested: T, inflight: T },
    #[error(
        "amount received {amount_received:?} exceeds amount sent {amount_sent:?} (fees cannot be negative)"
    )]
    NegativeFee { amount_sent: T, amount_received: T },
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError<T>),
}

/// Balance at a single venue, tracking available and inflight amounts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct VenueBalance<T> {
    /// Assets ready for use at this venue.
    available: T,
    /// Assets that have left this venue but haven't yet arrived at the
    /// destination venue (e.g., shares being minted into tokens, tokens being
    /// redeemed into shares, or USDC being bridged).
    inflight: T,
}

impl<T> VenueBalance<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy,
{
    pub(super) fn total(self) -> Result<T, ArithmeticError<T>> {
        self.available + self.inflight
    }

    pub(super) fn new(available: T, inflight: T) -> Self {
        Self {
            available,
            inflight,
        }
    }

    #[cfg(test)]
    pub(super) fn available(self) -> T {
        self.available
    }

    #[cfg(test)]
    pub(super) fn inflight(self) -> T {
        self.inflight
    }
}

impl<T> VenueBalance<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero
        + std::fmt::Debug,
{
    pub(super) fn has_inflight(self) -> bool {
        !self.inflight.is_zero()
    }

    /// Move amount from available to inflight (assets leaving this venue).
    pub(super) fn move_to_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_available = (self.available - amount)?;

        if new_available.is_negative() {
            return Err(InventoryError::InsufficientAvailable {
                requested: amount,
                available: self.available,
            });
        }

        let new_inflight = (self.inflight + amount)?;

        Ok(Self {
            available: new_available,
            inflight: new_inflight,
        })
    }

    /// Remove amount from inflight (transfer completed or assets arrived at destination).
    pub(super) fn confirm_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_inflight = (self.inflight - amount)?;

        if new_inflight.is_negative() {
            return Err(InventoryError::InsufficientInflight {
                requested: amount,
                inflight: self.inflight,
            });
        }

        Ok(Self {
            available: self.available,
            inflight: new_inflight,
        })
    }

    /// Move amount from inflight back to available (transfer failed/cancelled).
    pub(super) fn cancel_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_inflight = (self.inflight - amount)?;

        if new_inflight.is_negative() {
            return Err(InventoryError::InsufficientInflight {
                requested: amount,
                inflight: self.inflight,
            });
        }

        let new_available = (self.available + amount)?;

        Ok(Self {
            available: new_available,
            inflight: new_inflight,
        })
    }

    /// Add amount to available (assets arriving at this venue).
    pub(super) fn add_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_available = (self.available + amount)?;

        Ok(Self {
            available: new_available,
            inflight: self.inflight,
        })
    }

    /// Remove amount from available.
    pub(super) fn remove_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_available = (self.available - amount)?;

        if new_available.is_negative() {
            return Err(InventoryError::InsufficientAvailable {
                requested: amount,
                available: self.available,
            });
        }

        Ok(Self {
            available: new_available,
            inflight: self.inflight,
        })
    }

    /// Apply a fetched venue snapshot to reconcile tracked balance.
    ///
    /// When inflight is zero, sets `available = snapshot_balance` to match reality.
    /// When inflight is non-zero, returns self unchanged - we cannot safely reconcile
    /// while transfers are in progress because the snapshot alone cannot distinguish
    /// between "transfer completed" vs "unrelated inventory change".
    pub(super) fn apply_snapshot(self, snapshot_balance: T) -> Self {
        if !self.inflight.is_zero() {
            debug!(
                inflight = ?self.inflight,
                "Skipping snapshot reconciliation due to non-zero inflight"
            );
            return self;
        }

        Self {
            available: snapshot_balance,
            inflight: self.inflight,
        }
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use st0x_execution::FractionalShares;

    use super::*;
    use crate::threshold::Usdc;

    fn equity_balance(available: i64, inflight: i64) -> VenueBalance<FractionalShares> {
        VenueBalance::new(
            FractionalShares::new(Decimal::from(available)),
            FractionalShares::new(Decimal::from(inflight)),
        )
    }

    fn usdc_balance(available: i64, inflight: i64) -> VenueBalance<Usdc> {
        VenueBalance::new(
            Usdc(Decimal::from(available)),
            Usdc(Decimal::from(inflight)),
        )
    }

    #[test]
    fn total_sums_available_and_inflight() {
        let balance = equity_balance(100, 50);
        assert_eq!(balance.total().unwrap().inner(), Decimal::from(150));
    }

    #[test]
    fn has_inflight_true_when_nonzero() {
        let balance = equity_balance(100, 50);
        assert!(balance.has_inflight());
    }

    #[test]
    fn has_inflight_false_when_zero() {
        let balance = equity_balance(100, 0);
        assert!(!balance.has_inflight());
    }

    #[test]
    fn move_to_inflight_transfers_from_available() {
        let balance = equity_balance(100, 0);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.move_to_inflight(amount).unwrap();

        assert_eq!(result.available().inner(), Decimal::from(70));
        assert_eq!(result.inflight().inner(), Decimal::from(30));
    }

    #[test]
    fn move_to_inflight_fails_when_insufficient() {
        let balance = equity_balance(10, 0);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.move_to_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn confirm_inflight_removes_from_inflight() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.confirm_inflight(amount).unwrap();

        assert_eq!(result.available().inner(), Decimal::from(100));
        assert_eq!(result.inflight().inner(), Decimal::from(20));
    }

    #[test]
    fn confirm_inflight_fails_when_insufficient() {
        let balance = equity_balance(100, 10);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.confirm_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn cancel_inflight_returns_to_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.cancel_inflight(amount).unwrap();

        assert_eq!(result.available().inner(), Decimal::from(130));
        assert_eq!(result.inflight().inner(), Decimal::from(20));
    }

    #[test]
    fn cancel_inflight_fails_when_insufficient() {
        let balance = equity_balance(100, 10);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.cancel_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn add_available_increases_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.add_available(amount).unwrap();

        assert_eq!(result.available().inner(), Decimal::from(130));
        assert_eq!(result.inflight().inner(), Decimal::from(50));
    }

    #[test]
    fn remove_available_decreases_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.remove_available(amount).unwrap();

        assert_eq!(result.available().inner(), Decimal::from(70));
        assert_eq!(result.inflight().inner(), Decimal::from(50));
    }

    #[test]
    fn remove_available_fails_when_insufficient() {
        let balance = equity_balance(10, 50);
        let amount = FractionalShares::new(Decimal::from(30));

        let result = balance.remove_available(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn usdc_balance_works_same_as_equity() {
        let balance = usdc_balance(100, 0);
        let amount = Usdc(Decimal::from(30));

        let result = balance.move_to_inflight(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(70));
        assert_eq!(result.inflight().0, Decimal::from(30));
    }

    #[test]
    fn apply_snapshot_skips_when_inflight_nonzero() {
        let balance = equity_balance(90, 10);
        let snapshot_balance = FractionalShares::new(Decimal::from(95));

        let result = balance.apply_snapshot(snapshot_balance);

        // Balance unchanged when inflight is non-zero
        assert_eq!(result.available().inner(), Decimal::from(90));
        assert_eq!(result.inflight().inner(), Decimal::from(10));
    }

    #[test]
    fn apply_snapshot_sets_available_when_inflight_zero() {
        let balance = equity_balance(100, 0);
        let snapshot_balance = FractionalShares::new(Decimal::from(75));

        let result = balance.apply_snapshot(snapshot_balance);

        assert_eq!(result.available().inner(), Decimal::from(75));
        assert_eq!(result.inflight().inner(), Decimal::ZERO);
    }

    #[test]
    fn apply_snapshot_works_with_usdc_when_inflight_zero() {
        let balance = usdc_balance(1000, 0);
        let snapshot_balance = Usdc(Decimal::from(950));

        let result = balance.apply_snapshot(snapshot_balance);

        assert_eq!(result.available().0, Decimal::from(950));
        assert_eq!(result.inflight().0, Decimal::ZERO);
    }
}
