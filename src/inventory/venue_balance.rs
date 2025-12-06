//! Generic venue balance tracking for inventory management.

use std::ops::{Add, Sub};

use serde::{Deserialize, Serialize};

use crate::shares::{ArithmeticError, FractionalShares, HasZero};
use crate::threshold::Usdc;

/// Error type for inventory operations.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
enum InventoryError<T> {
    #[error(
        "insufficient available balance: requested {requested}, but only {available} available"
    )]
    InsufficientAvailable { requested: T, available: T },
    #[error("insufficient inflight balance: requested {requested}, but only {inflight} inflight")]
    InsufficientInflight { requested: T, inflight: T },
    #[error(transparent)]
    Arithmetic(#[from] ArithmeticError<T>),
}

/// Balance at a single venue, tracking available and inflight amounts.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
struct VenueBalance<T> {
    /// Assets ready for use at this venue.
    available: T,
    /// Assets that have left this venue but haven't yet arrived at the
    /// destination venue (e.g., shares being minted into tokens, tokens being
    /// redeemed into shares, or USDC being bridged).
    inflight: T,
}

type EquityVenueBalance = VenueBalance<FractionalShares>;
type UsdcVenueBalance = VenueBalance<Usdc>;

impl<T> VenueBalance<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy,
{
    fn new(available: T, inflight: T) -> Self {
        Self {
            available,
            inflight,
        }
    }

    fn total(self) -> Result<T, ArithmeticError<T>> {
        self.available + self.inflight
    }

    fn available(self) -> T {
        self.available
    }

    fn inflight(self) -> T {
        self.inflight
    }
}

impl<T> VenueBalance<T>
where
    T: Add<Output = Result<T, ArithmeticError<T>>>
        + Sub<Output = Result<T, ArithmeticError<T>>>
        + Copy
        + HasZero,
{
    fn has_inflight(self) -> bool {
        !self.inflight.is_zero()
    }

    /// Move amount from available to inflight (assets leaving this venue).
    fn move_to_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
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
    fn confirm_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
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
    fn cancel_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
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
    fn add_available(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_available = (self.available + amount)?;

        Ok(Self {
            available: new_available,
            inflight: self.inflight,
        })
    }

    /// Remove amount from available.
    fn remove_available(self, amount: T) -> Result<Self, InventoryError<T>> {
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
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use super::*;

    fn equity_balance(available: i64, inflight: i64) -> EquityVenueBalance {
        VenueBalance::new(
            FractionalShares(Decimal::from(available)),
            FractionalShares(Decimal::from(inflight)),
        )
    }

    fn usdc_balance(available: i64, inflight: i64) -> UsdcVenueBalance {
        VenueBalance::new(
            Usdc(Decimal::from(available)),
            Usdc(Decimal::from(inflight)),
        )
    }

    #[test]
    fn total_sums_available_and_inflight() {
        let b = equity_balance(100, 50);
        assert_eq!(b.total().unwrap().0, Decimal::from(150));
    }

    #[test]
    fn has_inflight_true_when_nonzero() {
        let b = equity_balance(100, 50);
        assert!(b.has_inflight());
    }

    #[test]
    fn has_inflight_false_when_zero() {
        let b = equity_balance(100, 0);
        assert!(!b.has_inflight());
    }

    #[test]
    fn move_to_inflight_transfers_from_available() {
        let b = equity_balance(100, 0);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.move_to_inflight(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(70));
        assert_eq!(result.inflight().0, Decimal::from(30));
    }

    #[test]
    fn move_to_inflight_fails_when_insufficient() {
        let b = equity_balance(10, 0);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.move_to_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn confirm_inflight_removes_from_inflight() {
        let b = equity_balance(100, 50);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.confirm_inflight(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(100));
        assert_eq!(result.inflight().0, Decimal::from(20));
    }

    #[test]
    fn confirm_inflight_fails_when_insufficient() {
        let b = equity_balance(100, 10);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.confirm_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn cancel_inflight_returns_to_available() {
        let b = equity_balance(100, 50);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.cancel_inflight(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(130));
        assert_eq!(result.inflight().0, Decimal::from(20));
    }

    #[test]
    fn cancel_inflight_fails_when_insufficient() {
        let b = equity_balance(100, 10);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.cancel_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn add_available_increases_available() {
        let b = equity_balance(100, 50);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.add_available(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(130));
        assert_eq!(result.inflight().0, Decimal::from(50));
    }

    #[test]
    fn remove_available_decreases_available() {
        let b = equity_balance(100, 50);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.remove_available(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(70));
        assert_eq!(result.inflight().0, Decimal::from(50));
    }

    #[test]
    fn remove_available_fails_when_insufficient() {
        let b = equity_balance(10, 50);
        let amount = FractionalShares(Decimal::from(30));

        let result = b.remove_available(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn usdc_balance_works_same_as_equity() {
        let b = usdc_balance(100, 0);
        let amount = Usdc(Decimal::from(30));

        let result = b.move_to_inflight(amount).unwrap();

        assert_eq!(result.available().0, Decimal::from(70));
        assert_eq!(result.inflight().0, Decimal::from(30));
    }
}
