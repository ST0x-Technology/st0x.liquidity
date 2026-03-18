//! Generic venue balance tracking for inventory management.

use rain_math_float::FloatError;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use tracing::{debug, warn};

use st0x_execution::HasZero;

impl<T: HasZero> Default for VenueBalance<T> {
    fn default() -> Self {
        Self {
            available: T::ZERO,
            inflight: T::ZERO,
        }
    }
}

/// Error type for inventory operations.
#[derive(Debug, thiserror::Error)]
pub(crate) enum InventoryError<T> {
    #[error(
        "insufficient available balance: requested {requested:?}, but only {available:?} available"
    )]
    InsufficientAvailable { requested: T, available: T },
    #[error(
        "insufficient inflight balance: requested {requested:?}, but only {inflight:?} inflight"
    )]
    InsufficientInflight { requested: T, inflight: T },
    #[error("negative inflight value: {value:?}")]
    NegativeInflight { value: T },
    #[error("arithmetic error: {0}")]
    Arithmetic(#[from] FloatError),
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
    T: Add<Output = Result<T, FloatError>> + Sub<Output = Result<T, FloatError>> + Copy,
{
    pub(super) fn total(self) -> Result<T, FloatError> {
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
    T: Add<Output = Result<T, FloatError>>
        + Sub<Output = Result<T, FloatError>>
        + Copy
        + HasZero
        + std::fmt::Debug,
{
    pub(super) fn has_inflight(self) -> Result<bool, FloatError> {
        self.inflight.is_zero().map(|zero| !zero)
    }

    /// Move amount from available to inflight (assets leaving this venue).
    pub(super) fn move_to_inflight(self, amount: T) -> Result<Self, InventoryError<T>> {
        let new_available = (self.available - amount)?;

        if new_available.is_negative()? {
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

        if new_inflight.is_negative()? {
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

        if new_inflight.is_negative()? {
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

        if new_available.is_negative()? {
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

    /// Replace the inflight balance with a polled value from an external system.
    ///
    /// Unlike `move_to_inflight` which transfers from available, this directly
    /// sets the inflight amount -- used when the external system (Alpaca's
    /// tokenization API) reports pending operations. The available balance is
    /// unchanged because it was already set by a separate available snapshot.
    pub(super) fn set_inflight(self, inflight: T) -> Result<Self, InventoryError<T>> {
        if inflight.is_negative()? {
            return Err(InventoryError::NegativeInflight { value: inflight });
        }

        Ok(Self {
            available: self.available,
            inflight,
        })
    }

    /// Apply a fetched venue snapshot to reconcile tracked balance.
    ///
    /// When inflight is zero, sets `available = snapshot_balance` to match reality.
    /// When inflight is non-zero, returns self unchanged - we cannot safely reconcile
    /// while transfers are in progress because the snapshot alone cannot distinguish
    /// between "transfer completed" vs "unrelated inventory change".
    pub(super) fn apply_snapshot(self, snapshot_balance: T) -> Result<Self, FloatError> {
        if !self.inflight.is_zero()? {
            debug!(
                inflight = ?self.inflight,
                "Skipping snapshot reconciliation due to non-zero inflight"
            );
            return Ok(self);
        }

        Ok(Self {
            available: snapshot_balance,
            inflight: self.inflight,
        })
    }

    /// Force-apply a snapshot, clearing inflight and setting
    /// available to the snapshot balance.
    ///
    /// Used for recovery when reactor state is corrupted (e.g.,
    /// stuck inflight from a failed transfer that never completed).
    /// The snapshot represents actual venue reality, so we trust
    /// it unconditionally and discard any tracked inflight.
    ///
    /// Takes the triggering error as a witness to prevent blind
    /// usage -- callers must have an error in hand to justify
    /// bypassing the normal inflight guard.
    pub(super) fn force_apply_snapshot<E: std::fmt::Debug>(
        self,
        snapshot_balance: T,
        recovering_from: &E,
    ) -> Self {
        warn!(
            ?recovering_from,
            inflight = ?self.inflight,
            "Force-applying snapshot to recover from error, clearing inflight"
        );

        Self {
            available: snapshot_balance,
            inflight: T::ZERO,
        }
    }
}

#[cfg(test)]
mod tests {
    use st0x_execution::FractionalShares;

    use st0x_finance::Usdc;

    use super::*;
    use st0x_float_macro::float;

    fn equity_balance(available: i64, inflight: i64) -> VenueBalance<FractionalShares> {
        VenueBalance::new(
            FractionalShares::new(float!(&available.to_string())),
            FractionalShares::new(float!(&inflight.to_string())),
        )
    }

    fn usdc_balance(available: i64, inflight: i64) -> VenueBalance<Usdc> {
        VenueBalance::new(
            Usdc::new(float!(&available.to_string())),
            Usdc::new(float!(&inflight.to_string())),
        )
    }

    #[test]
    fn total_sums_available_and_inflight() {
        let balance = equity_balance(100, 50);
        assert!(balance.total().unwrap().inner().eq(float!(150)).unwrap());
    }

    #[test]
    fn has_inflight_true_when_nonzero() {
        let balance = equity_balance(100, 50);
        assert!(balance.has_inflight().unwrap());
    }

    #[test]
    fn has_inflight_false_when_zero() {
        let balance = equity_balance(100, 0);
        assert!(!balance.has_inflight().unwrap());
    }

    #[test]
    fn move_to_inflight_transfers_from_available() {
        let balance = equity_balance(100, 0);
        let amount = FractionalShares::new(float!(30));

        let result = balance.move_to_inflight(amount).unwrap();

        assert!(result.available().inner().eq(float!(70)).unwrap());
        assert!(result.inflight().inner().eq(float!(30)).unwrap());
    }

    #[test]
    fn move_to_inflight_fails_when_insufficient() {
        let balance = equity_balance(10, 0);
        let amount = FractionalShares::new(float!(30));

        let result = balance.move_to_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn confirm_inflight_removes_from_inflight() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(float!(30));

        let result = balance.confirm_inflight(amount).unwrap();

        assert!(result.available().inner().eq(float!(100)).unwrap());
        assert!(result.inflight().inner().eq(float!(20)).unwrap());
    }

    #[test]
    fn confirm_inflight_fails_when_insufficient() {
        let balance = equity_balance(100, 10);
        let amount = FractionalShares::new(float!(30));

        let result = balance.confirm_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn cancel_inflight_returns_to_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(float!(30));

        let result = balance.cancel_inflight(amount).unwrap();

        assert!(result.available().inner().eq(float!(130)).unwrap());
        assert!(result.inflight().inner().eq(float!(20)).unwrap());
    }

    #[test]
    fn cancel_inflight_fails_when_insufficient() {
        let balance = equity_balance(100, 10);
        let amount = FractionalShares::new(float!(30));

        let result = balance.cancel_inflight(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientInflight { .. }
        ));
    }

    #[test]
    fn add_available_increases_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(float!(30));

        let result = balance.add_available(amount).unwrap();

        assert!(result.available().inner().eq(float!(130)).unwrap());
        assert!(result.inflight().inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn remove_available_decreases_available() {
        let balance = equity_balance(100, 50);
        let amount = FractionalShares::new(float!(30));

        let result = balance.remove_available(amount).unwrap();

        assert!(result.available().inner().eq(float!(70)).unwrap());
        assert!(result.inflight().inner().eq(float!(50)).unwrap());
    }

    #[test]
    fn remove_available_fails_when_insufficient() {
        let balance = equity_balance(10, 50);
        let amount = FractionalShares::new(float!(30));

        let result = balance.remove_available(amount);

        assert!(matches!(
            result.unwrap_err(),
            InventoryError::InsufficientAvailable { .. }
        ));
    }

    #[test]
    fn usdc_balance_works_same_as_equity() {
        let balance = usdc_balance(100, 0);
        let amount = Usdc::new(float!(30));

        let result = balance.move_to_inflight(amount).unwrap();

        assert!(result.available().inner().eq(float!(70)).unwrap());
        assert!(result.inflight().inner().eq(float!(30)).unwrap());
    }

    #[test]
    fn apply_snapshot_skips_when_inflight_nonzero() {
        let balance = equity_balance(90, 10);
        let snapshot_balance = FractionalShares::new(float!(95));

        let result = balance.apply_snapshot(snapshot_balance).unwrap();

        // Balance unchanged when inflight is non-zero
        assert!(result.available().inner().eq(float!(90)).unwrap());
        assert!(result.inflight().inner().eq(float!(10)).unwrap());
    }

    #[test]
    fn apply_snapshot_sets_available_when_inflight_zero() {
        let balance = equity_balance(100, 0);
        let snapshot_balance = FractionalShares::new(float!(75));

        let result = balance.apply_snapshot(snapshot_balance).unwrap();

        assert!(result.available().inner().eq(float!(75)).unwrap());
        assert!(result.inflight().is_zero().unwrap());
    }

    #[test]
    fn apply_snapshot_works_with_usdc_when_inflight_zero() {
        let balance = usdc_balance(1000, 0);
        let snapshot_balance = Usdc::new(float!(950));

        let result = balance.apply_snapshot(snapshot_balance).unwrap();

        assert!(result.available().inner().eq(float!(950)).unwrap());
        assert!(result.inflight().is_zero().unwrap());
    }

    #[derive(Debug)]
    struct TestError {
        _reason: &'static str,
    }

    #[test]
    fn set_inflight_replaces_inflight_without_changing_available() {
        let balance = equity_balance(100, 10);

        let result = balance
            .set_inflight(FractionalShares::new(float!("25")))
            .unwrap();

        assert!(
            result.available().inner().eq(float!("100")).unwrap(),
            "set_inflight should not change available"
        );
        assert!(
            result.inflight().inner().eq(float!("25")).unwrap(),
            "set_inflight should replace inflight with the new value"
        );
    }

    #[test]
    fn set_inflight_to_zero_clears_inflight() {
        let balance = equity_balance(100, 30);

        let result = balance.set_inflight(FractionalShares::ZERO).unwrap();

        assert!(
            result.available().inner().eq(float!("100")).unwrap(),
            "set_inflight(ZERO) should not change available"
        );
        assert!(
            result.inflight().is_zero().unwrap(),
            "set_inflight(ZERO) should zero out inflight"
        );
    }

    #[test]
    fn force_apply_snapshot_clears_inflight() {
        let balance = equity_balance(90, 10);
        let snapshot_balance = FractionalShares::new(float!(95));
        let error = TestError {
            _reason: "stuck inflight",
        };

        let result = balance.force_apply_snapshot(snapshot_balance, &error);

        assert!(result.available().inner().eq(float!(95)).unwrap());
        assert!(result.inflight().is_zero().unwrap());
    }

    #[test]
    fn force_apply_snapshot_works_when_inflight_zero() {
        let balance = equity_balance(100, 0);
        let snapshot_balance = FractionalShares::new(float!(75));
        let error = TestError {
            _reason: "recovery",
        };

        let result = balance.force_apply_snapshot(snapshot_balance, &error);

        assert!(result.available().inner().eq(float!(75)).unwrap());
        assert!(result.inflight().is_zero().unwrap());
    }

    #[test]
    fn force_apply_snapshot_works_with_usdc() {
        let balance = usdc_balance(1000, 200);
        let snapshot_balance = Usdc::new(float!(950));
        let error = TestError {
            _reason: "usdc corruption",
        };

        let result = balance.force_apply_snapshot(snapshot_balance, &error);

        assert!(result.available().inner().eq(float!(950)).unwrap());
        assert!(result.inflight().is_zero().unwrap());
    }
}
