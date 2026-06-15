//! USDC-specific trigger types and logic.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use chrono::{DateTime, Utc};
use rain_math_float::{Float, FloatError};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace, warn};

use st0x_finance::{Usd, Usdc};

use super::{RebalancingService, RebalancingServiceError};
use crate::conductor::job::{Job, JobQueue, Label, QueuePushError};
use crate::inventory::{
    BroadcastingInventory, Imbalance, ImbalanceThreshold, Inventory, TransferOp, Venue,
};
use crate::usdc_rebalance::{RebalanceDirection, UsdcRebalanceEvent, UsdcRebalanceId};

/// Dispatch decision returned by [`check_imbalance_and_build_operation`].
/// The trigger maps each variant to the corresponding apalis job queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UsdcRebalanceOperation {
    AlpacaToBase { amount: Usdc },
    BaseToAlpaca { amount: Usdc },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsdcTrackingEvent {
    Initiated,
    DepositConfirmed,
    ConversionConfirmed,
}

impl std::fmt::Display for UsdcTrackingEvent {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initiated => write!(formatter, "Initiated"),
            Self::DepositConfirmed => write!(formatter, "DepositConfirmed"),
            Self::ConversionConfirmed => write!(formatter, "ConversionConfirmed"),
        }
    }
}

#[derive(Debug, Clone)]
pub(super) struct UsdcRebalanceTracking {
    pub(super) direction: RebalanceDirection,
    pub(super) initiated_amount: Usdc,
    pub(super) bridged_amount_received: Option<Usdc>,
    pub(super) stage: UsdcRebalanceStage,
    pub(super) last_progress_at: DateTime<Utc>,
}

/// Single source of truth for the direction -> source-venue mapping: the venue
/// the USDC physically leaves. Both `UsdcRebalanceTracking::source_venue` and the
/// restart-safe operator-reconciliation path (which has no tracking instance to
/// call the method on) derive from this, so the two cannot drift.
fn source_venue(direction: &RebalanceDirection) -> Venue {
    match direction {
        RebalanceDirection::AlpacaToBase => Venue::Hedging,
        RebalanceDirection::BaseToAlpaca => Venue::MarketMaking,
    }
}

impl UsdcRebalanceTracking {
    pub(super) fn source_venue(&self) -> Venue {
        source_venue(&self.direction)
    }

    fn source_transfer_started(&self) -> bool {
        match self.direction {
            RebalanceDirection::AlpacaToBase => !matches!(
                self.stage,
                UsdcRebalanceStage::ConversionInitiated | UsdcRebalanceStage::ConversionConfirmed
            ),
            RebalanceDirection::BaseToAlpaca => true,
        }
    }

    /// Whether the CCTP burn has irreversibly committed the funds. Past the
    /// burn the funds are no longer on the source venue, so a failure or timeout
    /// must NOT reconcile inflight back to source and must keep the guard.
    ///
    /// Defined per direction as "not a pre-burn stage", so any future stage
    /// defaults to post-burn (the safe, re-burn-blocking direction):
    /// - `AlpacaToBase` converts USD->USDC *before* withdrawing, so its
    ///   conversion stages are pre-burn; the burn begins at `BridgingInitiated`.
    /// - `BaseToAlpaca`'s only conversion is the *post-deposit* USDC->USD leg
    ///   (post-mint), so every stage except the pre-bridge withdrawal is
    ///   post-burn -- including the conversion stages, even though
    ///   `ConversionInitiated` resets the tracked stage after the deposit.
    pub(super) fn is_post_burn(&self) -> bool {
        use UsdcRebalanceStage::*;

        match self.direction {
            RebalanceDirection::AlpacaToBase => !matches!(
                self.stage,
                ConversionInitiated | ConversionConfirmed | Initiated | WithdrawalConfirmed
            ),
            RebalanceDirection::BaseToAlpaca => {
                !matches!(self.stage, Initiated | WithdrawalConfirmed)
            }
        }
    }

    fn track_progress(&mut self, event: &UsdcRebalanceEvent) {
        let Some(stage) = UsdcRebalanceStage::from_event(event) else {
            return;
        };
        let Some(last_progress_at) = stage.timestamp(event) else {
            return;
        };

        self.stage = stage;
        self.last_progress_at = last_progress_at;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UsdcTerminalAction {
    NotTerminal,
    Clear,
    PreservePostBurn,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum UsdcRebalanceStage {
    ConversionInitiated,
    ConversionConfirmed,
    Initiated,
    WithdrawalConfirmed,
    BridgingInitiated,
    BridgeAttestationReceived,
    Bridged,
    DepositInitiated,
    DepositConfirmed,
}

impl std::fmt::Display for UsdcRebalanceStage {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConversionInitiated => write!(formatter, "ConversionInitiated"),
            Self::ConversionConfirmed => write!(formatter, "ConversionConfirmed"),
            Self::Initiated => write!(formatter, "Initiated"),
            Self::WithdrawalConfirmed => write!(formatter, "WithdrawalConfirmed"),
            Self::BridgingInitiated => write!(formatter, "BridgingInitiated"),
            Self::BridgeAttestationReceived => write!(formatter, "BridgeAttestationReceived"),
            Self::Bridged => write!(formatter, "Bridged"),
            Self::DepositInitiated => write!(formatter, "DepositInitiated"),
            Self::DepositConfirmed => write!(formatter, "DepositConfirmed"),
        }
    }
}

impl UsdcRebalanceStage {
    pub(super) fn from_event(event: &UsdcRebalanceEvent) -> Option<Self> {
        use UsdcRebalanceEvent::*;

        match event {
            ConversionInitiated { .. } => Some(Self::ConversionInitiated),
            ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            } => Some(Self::ConversionConfirmed),
            Initiated { .. } => Some(Self::Initiated),
            WithdrawalConfirmed { .. } => Some(Self::WithdrawalConfirmed),
            BridgingInitiated { .. } => Some(Self::BridgingInitiated),
            BridgeAttestationReceived { .. } => Some(Self::BridgeAttestationReceived),
            Bridged { .. } | BridgingCompletionRecovered { .. } => Some(Self::Bridged),
            DepositInitiated { .. } => Some(Self::DepositInitiated),
            DepositConfirmed { .. } => Some(Self::DepositConfirmed),
            // Transient intent markers: immediately superseded by Initiated /
            // BridgingInitiated, so they emit no distinct progress stage.
            WithdrawalSubmitting { .. }
            | BridgingSubmitting { .. }
            | AttestationTimedOut { .. }
            | ConversionConfirmed { .. }
            | ConversionFailed { .. }
            | WithdrawalFailed { .. }
            | BridgingFailed { .. }
            | DepositFailed { .. }
            | OperatorReconciled { .. } => None,
        }
    }

    fn timestamp(self, event: &UsdcRebalanceEvent) -> Option<DateTime<Utc>> {
        use UsdcRebalanceEvent::*;

        match (self, event) {
            (Self::ConversionInitiated, ConversionInitiated { initiated_at, .. }) => {
                Some(*initiated_at)
            }
            (Self::ConversionConfirmed, ConversionConfirmed { converted_at, .. }) => {
                Some(*converted_at)
            }
            (Self::Initiated, Initiated { initiated_at, .. }) => Some(*initiated_at),
            (Self::WithdrawalConfirmed, WithdrawalConfirmed { confirmed_at, .. }) => {
                Some(*confirmed_at)
            }
            (Self::BridgingInitiated, BridgingInitiated { burned_at, .. }) => Some(*burned_at),
            (Self::BridgeAttestationReceived, BridgeAttestationReceived { attested_at, .. }) => {
                Some(*attested_at)
            }
            (Self::Bridged, Bridged { minted_at, .. }) => Some(*minted_at),
            (Self::Bridged, BridgingCompletionRecovered { recovered_at, .. }) => {
                Some(*recovered_at)
            }
            (
                Self::DepositInitiated,
                DepositInitiated {
                    deposit_initiated_at,
                    ..
                },
            ) => Some(*deposit_initiated_at),
            (
                Self::DepositConfirmed,
                DepositConfirmed {
                    deposit_confirmed_at,
                    ..
                },
            ) => Some(*deposit_confirmed_at),
            _ => None,
        }
    }
}

pub(crate) use st0x_config::ALPACA_MINIMUM_WITHDRAWAL;

/// Maximum decimal places for rebalanceable USDC token amounts.
const USDC_TRANSFER_MAX_DECIMAL_PLACES: u8 = 6;

/// Why a USDC trigger check did not produce an operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum UsdcTriggerSkip {
    /// No USDC imbalance detected.
    NoImbalance,
    /// Alpaca did not report settled withdrawable cash, so outbound
    /// Alpaca-to-Base capacity cannot be proven reserve-safe.
    MissingWithdrawableCash,
    /// Withdrawable cash is at or below the configured reserve, so no
    /// Alpaca-to-Base capacity is available. Operators should investigate;
    /// the reserve may need to be lowered or the broker rebalanced.
    ReserveCapacityExhausted,
    /// Imbalance exists but amount is below Alpaca's minimum withdrawal ($51).
    BelowMinimumWithdrawal { excess: Usdc },
    /// Arithmetic error during imbalance calculation.
    ArithmeticError,
}

/// RAII guard that holds a USDC in-progress claim.
/// Automatically releases the claim on drop unless `defuse` is called.
pub(super) struct InProgressGuard {
    /// Shared reference to the in-progress flag for cleanup on drop.
    in_progress: Arc<AtomicBool>,
    /// When true, the guard will not release the claim on drop.
    defused: bool,
}

impl InProgressGuard {
    /// Attempts to claim the USDC in-progress slot.
    /// Returns `None` if already claimed by another operation.
    pub(super) fn try_claim(in_progress: Arc<AtomicBool>) -> Option<Self> {
        let was_in_progress =
            in_progress.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst);

        if was_in_progress.is_err() {
            return None;
        }

        Some(Self {
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
            self.in_progress.store(false, Ordering::SeqCst);
        }
    }
}

/// Checks inventory for USDC imbalance and returns the appropriate bridging operation.
///
/// Returns `UsdcRebalanceOperation::AlpacaToBase` if there's too much USDC in
/// Alpaca that needs to be bridged to Base, or `BaseToAlpaca` if there's too
/// much USDC on Base that needs to be bridged to Alpaca.
pub(super) async fn check_imbalance_and_build_operation(
    threshold: &ImbalanceThreshold,
    inventory: &Arc<BroadcastingInventory>,
    usdc_limit: Option<Usdc>,
    reserved: Option<Usd>,
) -> Result<UsdcRebalanceOperation, UsdcTriggerSkip> {
    let imbalance_check = {
        let inventory = inventory.read().await;
        let imbalance = inventory
            .check_usdc_imbalance_with_gross_offchain(threshold, reserved)
            .map_err(|error| {
                warn!(
                    target: "rebalance",
                    ?error,
                    "USDC imbalance check failed due to inventory error"
                );
                UsdcTriggerSkip::ArithmeticError
            })?;
        // Only compute the Alpaca-to-Base capacity when we will actually use
        // it: the imbalance is offchain-heavy AND a reserve is configured.
        // Capacity reads parse `withdrawable_cash_cents`, so binding it to
        // unrelated branches would let a malformed withdrawable value block
        // Base-to-Alpaca rebalancing, which is supposed to be independent of
        // withdrawable cash. Holding the same read guard while computing both
        // values preserves TOCTOU safety against polling-driven updates.
        let capacity = match (&imbalance, reserved) {
            (Some(Imbalance::TooMuchOffchain { .. }), Some(_)) => Some(
                inventory
                    .alpaca_to_base_usdc_capacity(reserved)
                    .map_err(|error| {
                        warn!(
                            target: "rebalance",
                            ?error,
                            "USDC Alpaca-to-Base capacity check failed due to inventory error"
                        );
                        UsdcTriggerSkip::ArithmeticError
                    })?,
            ),
            _ => None,
        };
        drop(inventory);
        (imbalance, capacity)
    };
    let (imbalance, alpaca_to_base_capacity) = imbalance_check;

    let Some(imbalance) = imbalance else {
        trace!(target: "rebalance", "No USDC imbalance detected (balanced, partial data, or inflight)");
        return Err(UsdcTriggerSkip::NoImbalance);
    };

    match imbalance {
        Imbalance::TooMuchOffchain { excess } => {
            // `alpaca_to_base_capacity` is `Some(capacity)` only when a
            // reserve is configured (see the capacity computation above);
            // when reserved is `None`, there is no reserve to protect and
            // the transfer is constrained only by `usdc_limit` and the
            // Alpaca minimum withdrawal.
            let capped_by_capacity = match alpaca_to_base_capacity {
                Some(capacity_opt) => {
                    let Some(capacity) = capacity_opt else {
                        warn!(
                            target: "rebalance",
                            excess = ?excess,
                            "Skipping Alpaca-to-Base USDC rebalance: broker did not report withdrawable cash; reserve-safety cannot be proven"
                        );
                        return Err(UsdcTriggerSkip::MissingWithdrawableCash);
                    };

                    // A reserve that leaves capacity below the Alpaca minimum
                    // withdrawal is operationally equivalent to capacity == 0
                    // — the bot cannot make ANY reserve-safe transfer. Report
                    // it with the precise reason rather than letting the
                    // result fall through to BelowMinimumWithdrawal, which
                    // would be misleading.
                    if capacity.lt(&ALPACA_MINIMUM_WITHDRAWAL).map_err(|error| {
                        warn!(
                            target: "rebalance",
                            ?error,
                            "USDC capacity minimum-comparison failed"
                        );
                        UsdcTriggerSkip::ArithmeticError
                    })? {
                        warn!(
                            target: "rebalance",
                            excess = ?excess,
                            ?reserved,
                            ?capacity,
                            minimum = ?*ALPACA_MINIMUM_WITHDRAWAL,
                            "Skipping Alpaca-to-Base USDC rebalance: reserve leaves withdrawable capacity below Alpaca minimum withdrawal"
                        );
                        return Err(UsdcTriggerSkip::ReserveCapacityExhausted);
                    }

                    let capped = cap_usdc(excess, usdc_limit);
                    cap_usdc_by_alpaca_capacity(capped, capacity).map_err(|error| {
                        warn!(
                            target: "rebalance",
                            ?error,
                            "USDC Alpaca capacity comparison failed"
                        );
                        UsdcTriggerSkip::ArithmeticError
                    })?
                }
                None => cap_usdc(excess, usdc_limit),
            };

            let capped = truncate_for_transfer(capped_by_capacity).map_err(|error| {
                warn!(
                    target: "rebalance",
                    ?error,
                    "Failed to truncate offchain USDC imbalance for transfer"
                );
                UsdcTriggerSkip::ArithmeticError
            })?;

            if capped.lt(&ALPACA_MINIMUM_WITHDRAWAL).map_err(|error| {
                warn!(
                    target: "rebalance",
                    ?error,
                    "USDC minimum-withdrawal comparison failed"
                );
                UsdcTriggerSkip::ArithmeticError
            })? {
                debug!(
                    target: "rebalance",
                    excess = ?capped,
                    minimum = ?*ALPACA_MINIMUM_WITHDRAWAL,
                    "USDC imbalance below Alpaca minimum withdrawal, skipping"
                );
                Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess: capped })
            } else {
                Ok(UsdcRebalanceOperation::AlpacaToBase { amount: capped })
            }
        }
        Imbalance::TooMuchOnchain { excess } => {
            let amount = truncate_for_transfer(cap_usdc(excess, usdc_limit)).map_err(|error| {
                warn!(
                    target: "rebalance",
                    ?error,
                    "Failed to truncate onchain USDC imbalance for transfer"
                );
                UsdcTriggerSkip::ArithmeticError
            })?;

            if amount.inner().is_zero().map_err(|error| {
                warn!(
                    target: "rebalance",
                    ?error,
                    "USDC truncated-amount zero-check failed"
                );
                UsdcTriggerSkip::ArithmeticError
            })? {
                debug!(
                    target: "rebalance",
                    excess = ?excess,
                    "Skipping onchain USDC rebalance because truncation collapsed the transfer to zero"
                );
                return Err(UsdcTriggerSkip::NoImbalance);
            }

            Ok(UsdcRebalanceOperation::BaseToAlpaca { amount })
        }
    }
}

fn cap_usdc(amount: Usdc, usdc_limit: Option<Usdc>) -> Usdc {
    let Some(cap) = usdc_limit else {
        return amount;
    };

    if amount.gt(&cap).unwrap_or(false) {
        warn!(
            target: "rebalance",
            computed = %amount,
            limit = %cap,
            "USDC rebalancing amount capped by operational limit"
        );
        cap
    } else {
        amount
    }
}

fn cap_usdc_by_alpaca_capacity(amount: Usdc, capacity: Usdc) -> Result<Usdc, FloatError> {
    if amount.gt(&capacity)? {
        warn!(
            target: "rebalance",
            computed = %amount,
            capacity = %capacity,
            "USDC rebalancing amount capped by Alpaca withdrawable cash after reserve"
        );
        Ok(capacity)
    } else {
        Ok(amount)
    }
}

fn truncate_for_transfer(amount: Usdc) -> Result<Usdc, FloatError> {
    let (fixed, _lossless) = amount
        .inner()
        .to_fixed_decimal_lossy(USDC_TRANSFER_MAX_DECIMAL_PLACES)?;
    let truncated_value = Float::from_fixed_decimal(fixed, USDC_TRANSFER_MAX_DECIMAL_PLACES)?;
    let truncated = Usdc::new(truncated_value);

    if truncated != amount {
        warn!(
            target: "rebalance",
            original = ?amount,
            truncated = ?truncated,
            "Truncated USDC rebalance amount to {} decimal places for transfer",
            USDC_TRANSFER_MAX_DECIMAL_PLACES
        );
    }

    Ok(truncated)
}

impl RebalancingService {
    pub(super) async fn on_usdc_rebalance(
        &self,
        id: UsdcRebalanceId,
        event: UsdcRebalanceEvent,
    ) -> Result<(), RebalancingServiceError> {
        let event_sync_guard = self.usdc_event_sync.lock().await;

        if self
            .timed_out_usdc_rebalances
            .read()
            .await
            .contains_key(&id)
        {
            warn!(target: "rebalance", id = %id, "Ignoring late USDC rebalance event after timeout cleanup");
            return Ok(());
        }

        let terminal_action = self.usdc_terminal_action(&id, &event).await;
        self.apply_usdc_rebalance_event(&id, &event, terminal_action)
            .await?;

        let is_clearable_terminal = terminal_action == UsdcTerminalAction::Clear;
        {
            let mut inventory = self.inventory.write().await;
            *inventory = if is_clearable_terminal {
                inventory.clone().clear_active_usdc_rebalance()
            } else {
                inventory.clone().set_active_usdc_rebalance(id.clone())
            };
        }
        if is_clearable_terminal {
            self.usdc_tracking.write().await.remove(&id);
            self.clear_usdc_in_progress();
            debug!(target: "rebalance", "Cleared USDC in-progress flag after rebalance terminal event");
        } else if terminal_action == UsdcTerminalAction::PreservePostBurn {
            self.usdc_in_progress.store(true, Ordering::SeqCst);
            warn!(
                target: "rebalance",
                id = %id,
                ?event,
                "Preserving USDC in-progress guard after post-burn terminal failure"
            );
        }

        drop(event_sync_guard);

        // A terminal USDC transfer cleared the in-progress claim, so we
        // cancel any pre-rebalance checks and push a fresh one against
        // the post-rebalance inventory.
        if is_clearable_terminal {
            self.equity_scheduler.cancel_pending().await;
            self.usdc_scheduler.cancel_pending().await;
            self.usdc_scheduler.enqueue_check().await;
        }

        Ok(())
    }

    async fn usdc_terminal_action(
        &self,
        id: &UsdcRebalanceId,
        event: &UsdcRebalanceEvent,
    ) -> UsdcTerminalAction {
        if !Self::is_terminal_usdc_rebalance_event(event) {
            return UsdcTerminalAction::NotTerminal;
        }

        if self.post_burn_failure(id, event).await {
            UsdcTerminalAction::PreservePostBurn
        } else {
            UsdcTerminalAction::Clear
        }
    }

    /// Whether a terminal failure event happened after the CCTP burn, so its
    /// funds cannot be reconciled to source and the guard must be kept.
    async fn post_burn_failure(&self, id: &UsdcRebalanceId, event: &UsdcRebalanceEvent) -> bool {
        use UsdcRebalanceEvent::*;

        // A recorded burn hash OR a CCTP nonce proves post-burn even if tracking
        // was lost: both are only ever populated after the burn reaches CCTP, so
        // either is irreversible evidence the guard must outlive a restart.
        if matches!(
            event,
            BridgingFailed {
                burn_tx_hash: Some(_),
                ..
            } | BridgingFailed {
                cctp_nonce: Some(_),
                ..
            }
        ) {
            return true;
        }

        // Every terminal failure is post-burn iff the tracked transfer is past
        // the burn. `is_post_burn` is direction-aware, so a BaseToAlpaca
        // post-deposit `ConversionFailed` (post-mint) is correctly preserved
        // while an AlpacaToBase pre-withdrawal `ConversionFailed` clears.
        // Terminal successes fall through to `false` and settle normally.
        //
        // When in-memory tracking is absent -- e.g. an apalis job that resumes
        // after a restart, where `recover_usdc_guard` reasserts the guard but
        // does not rebuild tracking -- we must NOT speculatively clear the guard
        // on a failure that could be post-burn. So:
        //   - `DepositFailed` is only reachable from `DepositInitiated`
        //     (post-mint), hence unconditionally post-burn.
        //   - `ConversionFailed` defaults to preserve when tracking is absent
        //     (the BaseToAlpaca post-deposit leg must hold; the rare lost-track
        //     AlpacaToBase pre-burn case holds conservatively rather than risk a
        //     re-burn -- safe, at worst it wedges that one rebalance until an
        //     operator clears it).
        //   - `WithdrawalFailed` / pre-burn `BridgingFailed` are always pre-burn,
        //     so absent tracking correctly clears.
        match event {
            DepositFailed { .. } => true,
            ConversionFailed { .. } => self
                .usdc_tracking
                .read()
                .await
                .get(id)
                .is_none_or(UsdcRebalanceTracking::is_post_burn),
            WithdrawalFailed { .. } | BridgingFailed { .. } => self
                .usdc_tracking
                .read()
                .await
                .get(id)
                .is_some_and(UsdcRebalanceTracking::is_post_burn),
            _ => false,
        }
    }

    async fn apply_usdc_rebalance_event(
        &self,
        id: &UsdcRebalanceId,
        event: &UsdcRebalanceEvent,
        terminal_action: UsdcTerminalAction,
    ) -> Result<(), RebalancingServiceError> {
        use UsdcRebalanceEvent::*;

        match event {
            ConversionInitiated {
                direction, amount, ..
            } => {
                self.upsert_conversion_tracking(id, direction, *amount, event)
                    .await;
            }
            Initiated {
                direction, amount, ..
            } => {
                self.track_initiated_usdc_rebalance(id, direction, *amount, event)
                    .await?;
            }
            WithdrawalConfirmed { .. }
            | BridgingInitiated { .. }
            | BridgeAttestationReceived { .. }
            | DepositInitiated { .. }
            | DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                ..
            }
            | ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            } => {
                self.track_usdc_stage_progress(id, event).await;
            }
            Bridged {
                amount_received, ..
            }
            | BridgingCompletionRecovered {
                amount_received, ..
            } => {
                // `BridgingCompletionRecovered` un-fails a post-burn
                // `BridgingFailed` back to `Bridged`: the mint is now confirmed,
                // so record the bridged amount and advance stage progress just
                // as a first-time `Bridged` would. The in-progress guard stays
                // held (recovery is mid-flight) and clears on the terminal
                // deposit, exactly like the normal bridge path.
                self.track_bridged_amount(id, *amount_received).await?;
                self.track_usdc_stage_progress(id, event).await;
            }
            ConversionConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                filled_amount,
                ..
            } => {
                self.complete_usdc_rebalance(
                    id,
                    UsdcTrackingEvent::ConversionConfirmed,
                    *filled_amount,
                )
                .await?;
            }
            DepositConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            } => {
                self.complete_alpaca_to_base_deposit(id).await?;
            }
            // Transient intent markers persisted before the on-chain withdraw /
            // burn. Detailed stage tracking starts at the subsequent Initiated /
            // BridgingInitiated; the inventory's active-rebalance claim is set by
            // the caller on this (first non-terminal) event, so nothing to do here.
            WithdrawalSubmitting { .. }
            | BridgingSubmitting { .. }
            | AttestationTimedOut { .. } => {}
            // Withdrawal failure is always pre-burn -> reconcile to source.
            WithdrawalFailed { .. } => {
                self.cancel_tracked_usdc_rebalance(id).await?;
            }
            // These failures may be pre- or post-burn (BridgingFailed by burn
            // stage; ConversionFailed by direction -- BaseToAlpaca's conversion
            // is post-deposit; DepositFailed is always post-mint). The classifier
            // decides: preserve post-burn, otherwise reconcile to source.
            BridgingFailed { .. } | DepositFailed { .. } | ConversionFailed { .. } => {
                if terminal_action == UsdcTerminalAction::PreservePostBurn {
                    // Preservation is achieved by NOT cancelling: the tracking
                    // entry, guard, and active id are all kept by the caller. This
                    // call only surfaces a diagnostic if tracking is absent.
                    self.warn_if_post_burn_tracking_missing(id).await;
                } else {
                    self.cancel_tracked_usdc_rebalance(id).await?;
                }
            }
            // Operator reconciliation of a post-burn `DepositFailed`: the minted
            // USDC left the source venue, so reconcile inflight with post-burn
            // semantics -- zero the source-venue inflight WITHOUT crediting
            // available -- never a `Cancel` (which would wrongly credit
            // available). Derives the source venue from `direction`, so it works
            // with tracking absent (post-restart). The caller's Clear terminal
            // action removes tracking and clears the guard.
            OperatorReconciled { direction, .. } => {
                self.reconcile_operator_resolved(direction, Utc::now())
                    .await?;
            }
        }

        Ok(())
    }

    /// Reconciles source-venue USDC inflight for an operator-reconciled,
    /// post-burn rebalance: zeroes the source-venue inflight while leaving
    /// `available` unchanged, because the funds physically left the source.
    /// Derives the source venue from `direction` via the shared `source_venue`
    /// mapping so it does not depend on in-memory tracking, which may be absent
    /// after a restart.
    async fn reconcile_operator_resolved(
        &self,
        direction: &RebalanceDirection,
        now: DateTime<Utc>,
    ) -> Result<(), RebalancingServiceError> {
        let mut inventory = self.inventory.write().await;
        *inventory = inventory
            .clone()
            .clear_usdc_inflight(source_venue(direction), now)?;
        drop(inventory);

        Ok(())
    }

    async fn warn_if_post_burn_tracking_missing(&self, id: &UsdcRebalanceId) {
        if self.usdc_tracking.read().await.contains_key(id) {
            return;
        }

        warn!(
            target: "rebalance",
            id = %id,
            "Post-burn terminal failure had no USDC tracking context to preserve"
        );
    }

    async fn track_initiated_usdc_rebalance(
        &self,
        id: &UsdcRebalanceId,
        direction: &RebalanceDirection,
        amount: Usdc,
        event: &UsdcRebalanceEvent,
    ) -> Result<(), RebalancingServiceError> {
        let stage = UsdcRebalanceStage::from_event(event).ok_or(
            RebalancingServiceError::MissingUsdcTrackingContext {
                id: id.clone(),
                event: UsdcTrackingEvent::Initiated,
            },
        )?;
        let last_progress_at =
            stage
                .timestamp(event)
                .ok_or(RebalancingServiceError::MissingUsdcTrackingContext {
                    id: id.clone(),
                    event: UsdcTrackingEvent::Initiated,
                })?;
        let mut tracking = self.usdc_tracking.write().await;
        if let Some(existing) = tracking.get_mut(id) {
            let tracking_entry = UsdcRebalanceTracking {
                direction: direction.clone(),
                initiated_amount: amount,
                bridged_amount_received: existing.bridged_amount_received,
                stage,
                last_progress_at,
            };

            if existing.source_transfer_started() {
                *existing = tracking_entry;
                return Ok(());
            }

            drop(tracking);

            let update =
                Inventory::transfer(tracking_entry.source_venue(), TransferOp::Start, amount);

            let mut inventory = self.inventory.write().await;
            *inventory = inventory.clone().update_usdc(update, Utc::now())?;
            drop(inventory);

            self.usdc_tracking
                .write()
                .await
                .insert(id.clone(), tracking_entry);

            return Ok(());
        }
        drop(tracking);

        let tracking_entry = UsdcRebalanceTracking {
            direction: direction.clone(),
            initiated_amount: amount,
            bridged_amount_received: None,
            stage,
            last_progress_at,
        };

        let update = Inventory::transfer(tracking_entry.source_venue(), TransferOp::Start, amount);

        let mut inventory = self.inventory.write().await;
        *inventory = inventory.clone().update_usdc(update, Utc::now())?;
        drop(inventory);

        self.usdc_tracking
            .write()
            .await
            .insert(id.clone(), tracking_entry);

        Ok(())
    }

    async fn upsert_conversion_tracking(
        &self,
        id: &UsdcRebalanceId,
        direction: &RebalanceDirection,
        amount: Usdc,
        event: &UsdcRebalanceEvent,
    ) {
        let Some(stage) = UsdcRebalanceStage::from_event(event) else {
            warn!(
                target: "rebalance",
                id = %id,
                ?event,
                "Skipping conversion tracking update: event yielded no timeout stage"
            );
            return;
        };
        let Some(last_progress_at) = stage.timestamp(event) else {
            warn!(
                target: "rebalance",
                id = %id,
                ?stage,
                ?event,
                "Skipping conversion tracking update: stage had no timestamp"
            );
            return;
        };
        let mut tracking = self.usdc_tracking.write().await;

        if let Some(existing) = tracking.get_mut(id) {
            existing.direction = direction.clone();
            if !existing.source_transfer_started() {
                existing.initiated_amount = amount;
            }
            existing.stage = stage;
            existing.last_progress_at = last_progress_at;
            return;
        }

        let tracking_entry = UsdcRebalanceTracking {
            direction: direction.clone(),
            initiated_amount: amount,
            bridged_amount_received: None,
            stage,
            last_progress_at,
        };

        tracking.insert(id.clone(), tracking_entry);
    }

    async fn track_bridged_amount(
        &self,
        id: &UsdcRebalanceId,
        amount_received: Usdc,
    ) -> Result<(), RebalancingServiceError> {
        let mut tracking = self.usdc_tracking.write().await;
        let Some(existing) = tracking.get_mut(id) else {
            // Resumed after a restart with no rebuilt tracking (e.g. a post-burn
            // BridgingFailed recovered via RecoverBridging): there is no in-memory
            // bridged amount to record, and the terminal success event clears the
            // guard without reconciliation. Warn and return Ok -- consistent with
            // complete_usdc_rebalance / complete_alpaca_to_base_deposit -- rather
            // than wedging the reactor on a MissingUsdcTrackingContext error.
            warn!(target: "rebalance", id = %id, "Bridged event missing USDC tracking context; skipping bridged-amount tracking (resumed after restart)");
            return Ok(());
        };

        existing.bridged_amount_received = Some(amount_received);
        drop(tracking);

        Ok(())
    }

    async fn complete_alpaca_to_base_deposit(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<(), RebalancingServiceError> {
        let Some(tracking) = self.usdc_tracking.read().await.get(id).cloned() else {
            // Resumed after a restart with no rebuilt tracking: the transfer
            // already settled, so there is no inventory inflight to reconcile.
            // Return Ok so the caller clears the guard (terminal success) rather
            // than wedging it forever on a MissingUsdcTrackingContext error.
            warn!(target: "rebalance", id = %id, "DepositConfirmed event missing USDC tracking context; clearing guard without reconciliation (resumed after restart)");
            return Ok(());
        };

        let Some(amount_received) = tracking.bridged_amount_received else {
            warn!(
                target: "rebalance",
                id = %id,
                "DepositConfirmed event missing bridged amount for USDC rebalance"
            );
            return Err(RebalancingServiceError::MissingUsdcBridgedAmount { id: id.clone() });
        };

        self.complete_usdc_rebalance(id, UsdcTrackingEvent::DepositConfirmed, amount_received)
            .await
    }

    async fn cancel_tracked_usdc_rebalance(
        &self,
        id: &UsdcRebalanceId,
    ) -> Result<(), RebalancingServiceError> {
        let Some(tracking) = self.usdc_tracking.read().await.get(id).cloned() else {
            debug!(
                target: "rebalance",
                id = %id,
                "Terminal failure event had no USDC tracking context to cancel"
            );
            return Ok(());
        };

        if !tracking.source_transfer_started() {
            return Ok(());
        }

        let source_venue = tracking.source_venue();
        let initiated_amount = tracking.initiated_amount;
        let now = Utc::now();
        let update = Box::new(move |inventory| {
            let cancelled =
                Inventory::transfer(source_venue, TransferOp::Cancel, initiated_amount)(inventory)?;
            Inventory::with_last_rebalancing(now)(cancelled)
        });

        let mut inventory = self.inventory.write().await;
        *inventory = inventory.clone().update_usdc(update, now)?;
        drop(inventory);

        Ok(())
    }

    async fn complete_usdc_rebalance(
        &self,
        id: &UsdcRebalanceId,
        event: UsdcTrackingEvent,
        settled_amount: Usdc,
    ) -> Result<(), RebalancingServiceError> {
        let Some(tracking) = self.usdc_tracking.read().await.get(id).cloned() else {
            // Resumed after a restart with no rebuilt tracking: the transfer
            // already settled, so there is no inventory inflight to reconcile.
            // Return Ok so the caller clears the guard (terminal success) rather
            // than wedging it forever on a MissingUsdcTrackingContext error.
            warn!(target: "rebalance", id = %id, ?event, "Terminal success event missing USDC tracking context; clearing guard without reconciliation (resumed after restart)");
            return Ok(());
        };

        let source_venue = tracking.source_venue();
        let initiated_amount = tracking.initiated_amount;

        if settled_amount.gt(&initiated_amount)? {
            warn!(
                target: "rebalance",
                id = %id,
                ?event,
                ?initiated_amount,
                ?settled_amount,
                "Settled USDC amount exceeds initiated amount"
            );
            return Err(RebalancingServiceError::SettledUsdcExceedsInitiatedAmount {
                id: id.clone(),
                event,
                initiated_amount,
                settled_amount,
            });
        }

        let now = Utc::now();
        let update = Box::new(move |inventory| {
            let settled =
                Inventory::settle_transfer(source_venue, initiated_amount, settled_amount)(
                    inventory,
                )?;
            Inventory::with_last_rebalancing(now)(settled)
        });

        let mut inventory = self.inventory.write().await;
        *inventory = inventory.clone().update_usdc(update, now)?;
        drop(inventory);

        Ok(())
    }

    async fn track_usdc_stage_progress(&self, id: &UsdcRebalanceId, event: &UsdcRebalanceEvent) {
        let tracked = {
            let mut tracking = self.usdc_tracking.write().await;
            tracking.get_mut(id).is_some_and(|existing| {
                existing.track_progress(event);
                true
            })
        };

        if !tracked {
            warn!(target: "rebalance", id = %id, "USDC progress event missing tracking context");
        }
    }
}

/// USDC rebalancing check. Payload-less because USDC is a single
/// global balance, not per-symbol.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub(crate) struct UsdcRebalancingCheck;

pub(crate) type UsdcRebalancingCheckJobQueue = JobQueue<UsdcRebalancingCheck>;

/// USDC checks never propagate errors: the trigger logs and skips on
/// arithmetic/threshold issues, matching the pre-job direct-call
/// semantics.
#[derive(Debug, thiserror::Error)]
pub(crate) enum UsdcRebalancingCheckJobError {}

impl Job<RebalancingService> for UsdcRebalancingCheck {
    type Output = ();
    type Error = UsdcRebalancingCheckJobError;

    const WORKER_NAME: &'static str = "usdc-rebalancing-check-worker";

    #[cfg(any(test, feature = "test-support"))]
    const JOB_KIND: crate::conductor::job::JobKind =
        crate::conductor::job::JobKind::UsdcRebalancingCheck;

    fn label(&self) -> Label {
        Label::new("UsdcRebalancingCheck")
    }

    async fn perform(&self, trigger: &RebalancingService) -> Result<Self::Output, Self::Error> {
        trigger.check_and_trigger_usdc().await;
        Ok(())
    }
}

/// Owns the USDC-check queue and the domain-level operations
/// callers (the reactor, the conductor wiring) want: enqueue a check,
/// cancel pending checks after a terminal event. Keeps queue plumbing
/// out of [`RebalancingService`] and out of the conductor wiring.
#[derive(Clone)]
pub(crate) struct UsdcRebalancingCheckScheduler {
    queue: UsdcRebalancingCheckJobQueue,
}

impl UsdcRebalancingCheckScheduler {
    pub(crate) fn new(pool: &apalis_sqlite::SqlitePool) -> Self {
        Self {
            queue: UsdcRebalancingCheckJobQueue::new(pool),
        }
    }

    pub(crate) fn queue(&self) -> &UsdcRebalancingCheckJobQueue {
        &self.queue
    }

    /// Best-effort enqueue. Failures are logged: an enqueue miss only
    /// delays the next imbalance check, which the next snapshot will
    /// re-trigger.
    pub(super) async fn enqueue_check(&self) {
        let mut queue = self.queue.clone();
        if let Err(QueuePushError(error)) = queue.push(UsdcRebalancingCheck).await {
            warn!(target: "rebalance", %error, "Failed to enqueue UsdcRebalancingCheck job");
        }
    }

    pub(super) async fn cancel_pending(&self) {
        self.queue.cancel_all_pending().await;
    }
}

/// Test helper: synchronously drain every pending USDC-check row the
/// service enqueued, running each job's [`Job::perform`] and marking
/// the row `Done`.
#[cfg(test)]
pub(crate) async fn drain_pending_usdc_jobs(service: &Arc<RebalancingService>) -> usize {
    let pool = service.usdc_scheduler.queue().pool().clone();
    let mut processed = 0usize;

    let job_type = std::any::type_name::<UsdcRebalancingCheck>();

    loop {
        let row: Option<(String, Vec<u8>)> = sqlx_apalis::query_as(
            "SELECT id, job FROM Jobs \
             WHERE status = 'Pending' AND job_type = ? \
             ORDER BY run_at LIMIT 1",
        )
        .bind(job_type)
        .fetch_optional(&pool)
        .await
        .expect("query pending usdc-check jobs");

        let Some((id, payload)) = row else {
            break;
        };

        let job: UsdcRebalancingCheck =
            serde_json::from_slice(&payload).expect("deserialize UsdcRebalancingCheck payload");
        match job.perform(service).await {
            Ok(()) => {}
            Err(never) => match never {},
        }

        sqlx_apalis::query("UPDATE Jobs SET status = 'Done' WHERE id = ?")
            .bind(&id)
            .execute(&pool)
            .await
            .expect("mark usdc-check job done");

        processed += 1;
    }

    processed
}

#[cfg(test)]
mod tests {
    use alloy::primitives::{B256, TxHash};
    use chrono::TimeZone;
    use tokio::sync::broadcast;
    use uuid::Uuid;

    use st0x_dto::Statement;
    use st0x_execution::ClientOrderId;
    use st0x_float_macro::float;

    use super::*;
    use crate::inventory::InventoryView;
    use crate::usdc_rebalance::TransferRef;

    #[test]
    fn test_guard_releases_on_drop() {
        let in_progress = Arc::new(AtomicBool::new(false));

        {
            let guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.load(Ordering::SeqCst));
            drop(guard);
        }

        assert!(!in_progress.load(Ordering::SeqCst));
    }

    #[test]
    fn test_guard_defuse_prevents_release() {
        let in_progress = Arc::new(AtomicBool::new(false));

        {
            let guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();
            assert!(in_progress.load(Ordering::SeqCst));
            guard.defuse();
        }

        // Should still be in progress after defused guard dropped
        assert!(in_progress.load(Ordering::SeqCst));
    }

    #[test]
    fn test_guard_try_claim_fails_when_already_claimed() {
        let in_progress = Arc::new(AtomicBool::new(false));

        let _guard = InProgressGuard::try_claim(Arc::clone(&in_progress)).unwrap();

        let second_claim = InProgressGuard::try_claim(Arc::clone(&in_progress));
        assert!(second_claim.is_none());
    }

    #[tokio::test]
    async fn test_balanced_inventory_returns_no_imbalance() {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default(),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None, None).await;

        assert_eq!(result, Err(UsdcTriggerSkip::NoImbalance));
    }

    #[test]
    fn alpaca_minimum_withdrawal_is_51_usdc() {
        assert_eq!(
            *ALPACA_MINIMUM_WITHDRAWAL,
            Usdc::new(float!(51)),
            "Alpaca minimum withdrawal should be $51 to account for ~17bps USDC/USD spread"
        );
    }

    #[tokio::test]
    async fn test_excess_below_minimum_returns_below_minimum_withdrawal() {
        // 90 offchain, 10 onchain = 90% offchain
        // Excess to reach 50% target = 90 - 50 = 40 USDC (below $51 minimum)
        let inventory = InventoryView::default()
            .with_usdc(Usdc::new(float!(10)), Usdc::new(float!(90)))
            .with_withdrawable_cash_cents(9000);

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None, None).await;

        assert!(
            matches!(result, Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess }) if excess.inner().lt(float!(51)).unwrap_or(false)),
            "Expected BelowMinimumWithdrawal, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_excess_above_minimum_triggers_alpaca_to_base() {
        // 500 offchain, 100 onchain = 83% offchain
        // To reach 50% target: need 300 each, so excess = 500 - 300 = 200 USDC
        let inventory = InventoryView::default()
            .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
            .with_withdrawable_cash_cents(50_000);

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None, None).await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if !amount.inner().lt(float!(51)).unwrap_or(true)),
            "Expected AlpacaToBase with amount >= 51, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_excess_at_exactly_minimum_triggers_alpaca_to_base() {
        // Edge case: excess is exactly $51
        // Total = 102, target = 51 each
        // If offchain = 102, onchain = 0, excess = 102 - 51 = 51
        let inventory = InventoryView::default()
            .with_usdc(Usdc::new(float!(0)), Usdc::new(float!(102)))
            .with_withdrawable_cash_cents(10_200);

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None, None).await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(51))),
            "Expected AlpacaToBase with amount = 51, got {result:?}"
        );
    }

    #[tokio::test]
    async fn test_too_much_onchain_has_no_minimum_restriction() {
        // When there's too much onchain (Base), we deposit to Alpaca.
        // Deposits have no minimum restriction (verified in production at 0.01 USDC).
        // 10 offchain, 90 onchain = 10% offchain (below 30% lower bound)
        // Excess = 90 - 50 = 40 USDC (would be below withdrawal minimum, but deposits are fine)
        let inventory =
            InventoryView::default().with_usdc(Usdc::new(float!(90)), Usdc::new(float!(10)));

        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(&threshold, &inventory, None, None).await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::BaseToAlpaca { amount }) if amount == Usdc::new(float!(40))),
            "Expected BaseToAlpaca with amount = 40 (no minimum for deposits), got {result:?}"
        );
    }

    #[tokio::test]
    async fn operational_limits_cap_usdc_amount() {
        let inventory = InventoryView::default()
            .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
            .with_withdrawable_cash_cents(50_000);
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(inventory, event_sender));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(100)));

        let result =
            check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit, None).await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Operational limit should cap USDC transfer to 100, got {result:?}"
        );
    }

    #[tokio::test]
    async fn capped_usdc_rebalancing_leaves_remaining_imbalance_triggerable() {
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(100)));

        // 100 onchain / 500 offchain -> 83% offchain, excess = 200
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_withdrawable_cash_cents(50_000),
            event_sender,
        ));

        let first =
            check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit, None).await;
        assert!(
            matches!(first, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "First transfer capped to 100, got {first:?}"
        );

        // After transferring 100: 200 onchain / 400 offchain -> 67% offchain
        // Still above 70% threshold? No - 400/600 = 66.7%, within 30%-70%. No trigger.
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let after_first = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(200)), Usdc::new(float!(400)))
                .with_withdrawable_cash_cents(40_000),
            event_sender,
        ));

        let second =
            check_imbalance_and_build_operation(&threshold, &after_first, usdc_limit, None).await;
        assert_eq!(
            second,
            Err(UsdcTriggerSkip::NoImbalance),
            "After 100 USDC transfer, 66.7% offchain is within bounds"
        );

        // But if only 50 was transferred: 150 onchain / 450 offchain -> 75% offchain
        // Still above 70%, so triggers again
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let partially_resolved = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(150)), Usdc::new(float!(450)))
                .with_withdrawable_cash_cents(45_000),
            event_sender,
        ));

        let third =
            check_imbalance_and_build_operation(&threshold, &partially_resolved, usdc_limit, None)
                .await;
        assert!(
            matches!(third, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Remaining imbalance triggers another capped transfer, got {third:?}"
        );
    }

    #[tokio::test]
    async fn capped_amount_below_minimum_skips_withdrawal() {
        // excess = $200 (above $51 minimum), but limit = $30 caps it below minimum
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_withdrawable_cash_cents(50_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let usdc_limit = Some(Usdc::new(float!(30)));

        let result =
            check_imbalance_and_build_operation(&threshold, &inventory, usdc_limit, None).await;

        assert!(
            matches!(
                result,
                Err(UsdcTriggerSkip::BelowMinimumWithdrawal { excess })
                    if excess == Usdc::new(float!(30))
            ),
            "Cap of $30 should produce BelowMinimumWithdrawal, got {result:?}"
        );
    }

    #[tokio::test]
    async fn gross_offchain_cash_is_used_for_usdc_ratio_even_with_reserve() {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(12600)), Usdc::new(float!(800)))
                .with_offchain_gross_usd_cents(2_580_000)
                .with_withdrawable_cash_cents(2_580_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(25000))),
        )
        .await;

        assert_eq!(
            result,
            Err(UsdcTriggerSkip::NoImbalance),
            "Gross offchain cash should keep 12.6k/25.8k within the 30%-70% band"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_skips_when_withdrawable_cash_is_missing() {
        // Reserve configured + gross set (production invariant after first poll)
        // but withdrawable is missing -> capacity unknown -> skip.
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(250))),
        )
        .await;

        assert_eq!(result, Err(UsdcTriggerSkip::MissingWithdrawableCash));
    }

    #[tokio::test]
    async fn alpaca_to_base_is_capped_by_withdrawable_cash_after_reserve() {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000)
                .with_withdrawable_cash_cents(35_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(250))),
        )
        .await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Expected reserve-adjusted withdrawable capacity to cap transfer to 100, got {result:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_skips_when_reserve_exhausts_capacity() {
        // withdrawable == reserved -> capacity = 0 -> ReserveCapacityExhausted
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000)
                .with_withdrawable_cash_cents(2_500_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(25000))),
        )
        .await;

        assert_eq!(
            result,
            Err(UsdcTriggerSkip::ReserveCapacityExhausted),
            "withdrawable ({}) <= reserved ({}) should produce ReserveCapacityExhausted, got {result:?}",
            "25000",
            "25000"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_skips_when_reserve_exceeds_withdrawable() {
        // withdrawable < reserved -> capacity = 0 -> ReserveCapacityExhausted
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000)
                .with_withdrawable_cash_cents(2_000_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(25000))),
        )
        .await;

        assert_eq!(
            result,
            Err(UsdcTriggerSkip::ReserveCapacityExhausted),
            "reserved exceeding withdrawable should produce ReserveCapacityExhausted, got {result:?}"
        );
    }

    #[tokio::test]
    async fn alpaca_to_base_recovers_after_withdrawable_cash_returns() {
        // End-to-end recovery: a transient broker omission causes
        // MissingWithdrawableCash; once withdrawable cash returns, the next
        // trigger must succeed. Pins the conjunction of the
        // (TooMuchOffchain + reserve + missing-withdrawable) skip and the
        // post-recovery dispatch.
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };
        let reserved = Some(Usd::new(float!(250)));

        let before =
            check_imbalance_and_build_operation(&threshold, &inventory, None, reserved).await;
        assert_eq!(
            before,
            Err(UsdcTriggerSkip::MissingWithdrawableCash),
            "Missing withdrawable cash must skip with the precise reason"
        );

        // Polling-side recovery: withdrawable cash arrives with enough
        // headroom over the reserve to fund the transfer.
        {
            let mut guard = inventory.write().await;
            let taken = std::mem::take(&mut *guard);
            *guard = taken
                .with_usdc(Usdc::new(float!(100)), Usdc::new(float!(500)))
                .with_offchain_gross_usd_cents(50_000)
                .with_withdrawable_cash_cents(35_000);
        }

        let after =
            check_imbalance_and_build_operation(&threshold, &inventory, None, reserved).await;
        assert!(
            matches!(after, Ok(UsdcRebalanceOperation::AlpacaToBase { amount }) if amount == Usdc::new(float!(100))),
            "Post-recovery trigger must dispatch the reserve-aware capped transfer, got {after:?}"
        );
    }

    #[tokio::test]
    async fn imbalance_check_skips_when_gross_missing_with_reserve_configured() {
        // No offchain_gross_usd_cents, reserve configured -> skip (NoImbalance) rather
        // than fall back to net offchain in the ratio.
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(12600)), Usdc::new(float!(800)))
                .with_withdrawable_cash_cents(2_580_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(25000))),
        )
        .await;

        assert_eq!(
            result,
            Err(UsdcTriggerSkip::NoImbalance),
            "Reserve configured but gross offchain missing should skip rather than use net offchain, got {result:?}"
        );
    }

    #[tokio::test]
    async fn base_to_alpaca_is_not_capped_by_reserve_or_withdrawable_cash() {
        let (event_sender, _) = broadcast::channel::<Statement>(16);
        let inventory = Arc::new(BroadcastingInventory::new(
            InventoryView::default()
                .with_usdc(Usdc::new(float!(900)), Usdc::new(float!(100)))
                .with_offchain_gross_usd_cents(10_000),
            event_sender,
        ));
        let threshold = ImbalanceThreshold {
            target: float!(0.5),
            deviation: float!(0.2),
        };

        let result = check_imbalance_and_build_operation(
            &threshold,
            &inventory,
            None,
            Some(Usd::new(float!(100000))),
        )
        .await;

        assert!(
            matches!(result, Ok(UsdcRebalanceOperation::BaseToAlpaca { amount }) if amount == Usdc::new(float!(400))),
            "Expected Base-to-Alpaca transfer to ignore reserve and withdrawable cash, got {result:?}"
        );
    }

    fn ts(seconds: i64) -> DateTime<Utc> {
        Utc.timestamp_opt(seconds, 0).unwrap()
    }

    fn initiated_event(direction: RebalanceDirection, amount: Usdc) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref: TransferRef::OnchainTx(TxHash::ZERO),
            initiated_at: ts(100),
        }
    }

    fn conversion_initiated_event(
        direction: RebalanceDirection,
        amount: Usdc,
    ) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionInitiated {
            direction,
            amount,
            order_id: ClientOrderId::from_uuid(Uuid::nil()),
            initiated_at: ts(101),
        }
    }

    fn conversion_confirmed_event(
        direction: RebalanceDirection,
        filled_amount: Usdc,
    ) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionConfirmed {
            direction,
            filled_amount,
            converted_at: ts(102),
        }
    }

    fn withdrawal_confirmed_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: ts(103),
            withdrawal_tx: None,
        }
    }

    fn bridging_initiated_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: TxHash::ZERO,
            burned_at: ts(104),
        }
    }

    fn bridge_attestation_received_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![],
            cctp_nonce: B256::ZERO,
            message: None,
            mint_scan_from_block: Some(100),
            attested_at: ts(105),
        }
    }

    fn bridged_event(amount_received: Usdc) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::Bridged {
            mint_tx_hash: TxHash::ZERO,
            amount_received,
            fee_collected: Usdc::new(float!(0)),
            minted_at: ts(106),
        }
    }

    fn deposit_initiated_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::OnchainTx(TxHash::ZERO),
            deposit_initiated_at: ts(107),
        }
    }

    fn deposit_confirmed_event(direction: RebalanceDirection) -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositConfirmed {
            direction,
            deposit_confirmed_at: ts(108),
        }
    }

    fn conversion_failed_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::ConversionFailed {
            reason: "boom".into(),
            failed_at: ts(109),
        }
    }

    fn withdrawal_failed_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::WithdrawalFailed {
            reason: "boom".into(),
            failed_at: ts(110),
        }
    }

    fn bridging_failed_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "boom".into(),
            failed_at: ts(111),
        }
    }

    fn deposit_failed_event() -> UsdcRebalanceEvent {
        UsdcRebalanceEvent::DepositFailed {
            deposit_ref: None,
            reason: "boom".into(),
            failed_at: ts(112),
        }
    }

    #[test]
    fn truncate_for_transfer_preserves_value_within_six_decimals() {
        let original = Usdc::new(float!(123.456789));
        let result = truncate_for_transfer(original).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn truncate_for_transfer_drops_seventh_decimal() {
        let original = Usdc::new(float!(1.1234567));
        let result = truncate_for_transfer(original).unwrap();
        assert_eq!(result, Usdc::new(float!(1.123456)));
    }

    #[test]
    fn truncate_for_transfer_drops_excess_precision() {
        let original = Usdc::new(float!(99.123456789012345));
        let result = truncate_for_transfer(original).unwrap();
        assert_eq!(result, Usdc::new(float!(99.123456)));
    }

    #[test]
    fn truncate_for_transfer_zero_is_zero() {
        let result = truncate_for_transfer(Usdc::new(float!(0))).unwrap();
        assert_eq!(result, Usdc::new(float!(0)));
    }

    #[test]
    fn truncate_for_transfer_whole_dollar_is_unchanged() {
        let original = Usdc::new(float!(51));
        let result = truncate_for_transfer(original).unwrap();
        assert_eq!(result, original);
    }

    #[test]
    fn cap_usdc_returns_input_when_no_limit() {
        let amount = Usdc::new(float!(500));
        assert_eq!(cap_usdc(amount, None), amount);
    }

    #[test]
    fn cap_usdc_returns_input_when_below_limit() {
        let amount = Usdc::new(float!(100));
        let limit = Some(Usdc::new(float!(500)));
        assert_eq!(cap_usdc(amount, limit), amount);
    }

    #[test]
    fn cap_usdc_returns_input_when_equal_to_limit() {
        let amount = Usdc::new(float!(500));
        let limit = Some(Usdc::new(float!(500)));
        assert_eq!(cap_usdc(amount, limit), amount);
    }

    #[test]
    fn cap_usdc_returns_limit_when_above_limit() {
        let amount = Usdc::new(float!(1000));
        let limit_value = Usdc::new(float!(500));
        assert_eq!(cap_usdc(amount, Some(limit_value)), limit_value);
    }

    #[test]
    fn from_event_maps_conversion_initiated_to_stage() {
        let event =
            conversion_initiated_event(RebalanceDirection::AlpacaToBase, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::from_event(&event),
            Some(UsdcRebalanceStage::ConversionInitiated)
        );
    }

    #[test]
    fn from_event_maps_alpaca_to_base_conversion_confirmed_to_stage() {
        let event =
            conversion_confirmed_event(RebalanceDirection::AlpacaToBase, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::from_event(&event),
            Some(UsdcRebalanceStage::ConversionConfirmed)
        );
    }

    #[test]
    fn from_event_skips_base_to_alpaca_conversion_confirmed() {
        let event =
            conversion_confirmed_event(RebalanceDirection::BaseToAlpaca, Usdc::new(float!(1)));
        assert_eq!(UsdcRebalanceStage::from_event(&event), None);
    }

    #[test]
    fn from_event_maps_initiated_to_stage() {
        let event = initiated_event(RebalanceDirection::AlpacaToBase, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::from_event(&event),
            Some(UsdcRebalanceStage::Initiated)
        );
    }

    #[test]
    fn from_event_maps_withdrawal_confirmed_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&withdrawal_confirmed_event()),
            Some(UsdcRebalanceStage::WithdrawalConfirmed)
        );
    }

    #[test]
    fn from_event_maps_bridging_initiated_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&bridging_initiated_event()),
            Some(UsdcRebalanceStage::BridgingInitiated)
        );
    }

    #[test]
    fn from_event_maps_bridge_attestation_received_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&bridge_attestation_received_event()),
            Some(UsdcRebalanceStage::BridgeAttestationReceived)
        );
    }

    #[test]
    fn from_event_maps_bridged_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&bridged_event(Usdc::new(float!(99)))),
            Some(UsdcRebalanceStage::Bridged)
        );
    }

    #[test]
    fn from_event_maps_deposit_initiated_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&deposit_initiated_event()),
            Some(UsdcRebalanceStage::DepositInitiated)
        );
    }

    #[test]
    fn from_event_maps_deposit_confirmed_to_stage() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&deposit_confirmed_event(
                RebalanceDirection::AlpacaToBase
            )),
            Some(UsdcRebalanceStage::DepositConfirmed)
        );
    }

    #[test]
    fn from_event_skips_conversion_failed() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&conversion_failed_event()),
            None
        );
    }

    #[test]
    fn from_event_skips_withdrawal_failed() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&withdrawal_failed_event()),
            None
        );
    }

    #[test]
    fn from_event_skips_bridging_failed() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&bridging_failed_event()),
            None
        );
    }

    #[test]
    fn from_event_skips_deposit_failed() {
        assert_eq!(
            UsdcRebalanceStage::from_event(&deposit_failed_event()),
            None
        );
    }

    #[test]
    fn timestamp_extracts_initiated_at_from_conversion_initiated() {
        let event =
            conversion_initiated_event(RebalanceDirection::AlpacaToBase, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::ConversionInitiated.timestamp(&event),
            Some(ts(101))
        );
    }

    #[test]
    fn timestamp_extracts_converted_at_from_conversion_confirmed() {
        let event =
            conversion_confirmed_event(RebalanceDirection::AlpacaToBase, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::ConversionConfirmed.timestamp(&event),
            Some(ts(102))
        );
    }

    #[test]
    fn timestamp_extracts_initiated_at_from_initiated() {
        let event = initiated_event(RebalanceDirection::BaseToAlpaca, Usdc::new(float!(1)));
        assert_eq!(
            UsdcRebalanceStage::Initiated.timestamp(&event),
            Some(ts(100))
        );
    }

    #[test]
    fn timestamp_extracts_confirmed_at_from_withdrawal_confirmed() {
        assert_eq!(
            UsdcRebalanceStage::WithdrawalConfirmed.timestamp(&withdrawal_confirmed_event()),
            Some(ts(103))
        );
    }

    #[test]
    fn timestamp_extracts_burned_at_from_bridging_initiated() {
        assert_eq!(
            UsdcRebalanceStage::BridgingInitiated.timestamp(&bridging_initiated_event()),
            Some(ts(104))
        );
    }

    #[test]
    fn timestamp_extracts_attested_at_from_bridge_attestation_received() {
        assert_eq!(
            UsdcRebalanceStage::BridgeAttestationReceived
                .timestamp(&bridge_attestation_received_event()),
            Some(ts(105))
        );
    }

    #[test]
    fn timestamp_extracts_minted_at_from_bridged() {
        assert_eq!(
            UsdcRebalanceStage::Bridged.timestamp(&bridged_event(Usdc::new(float!(99)))),
            Some(ts(106))
        );
    }

    #[test]
    fn timestamp_extracts_deposit_initiated_at_from_deposit_initiated() {
        assert_eq!(
            UsdcRebalanceStage::DepositInitiated.timestamp(&deposit_initiated_event()),
            Some(ts(107))
        );
    }

    #[test]
    fn timestamp_extracts_deposit_confirmed_at_from_deposit_confirmed() {
        assert_eq!(
            UsdcRebalanceStage::DepositConfirmed
                .timestamp(&deposit_confirmed_event(RebalanceDirection::AlpacaToBase)),
            Some(ts(108))
        );
    }

    #[test]
    fn timestamp_returns_none_when_stage_does_not_match_event() {
        let event = withdrawal_confirmed_event();
        assert_eq!(UsdcRebalanceStage::Initiated.timestamp(&event), None);
    }

    fn tracking(direction: RebalanceDirection, stage: UsdcRebalanceStage) -> UsdcRebalanceTracking {
        UsdcRebalanceTracking {
            direction,
            initiated_amount: Usdc::new(float!(100)),
            bridged_amount_received: None,
            stage,
            last_progress_at: ts(0),
        }
    }

    #[test]
    fn source_venue_alpaca_to_base_is_hedging() {
        let entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::Initiated,
        );
        assert_eq!(entry.source_venue(), Venue::Hedging);
    }

    #[test]
    fn source_venue_base_to_alpaca_is_market_making() {
        let entry = tracking(
            RebalanceDirection::BaseToAlpaca,
            UsdcRebalanceStage::Initiated,
        );
        assert_eq!(entry.source_venue(), Venue::MarketMaking);
    }

    #[test]
    fn source_transfer_started_alpaca_to_base_false_during_conversion_initiated() {
        let entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::ConversionInitiated,
        );
        assert!(!entry.source_transfer_started());
    }

    #[test]
    fn source_transfer_started_alpaca_to_base_false_during_conversion_confirmed() {
        let entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::ConversionConfirmed,
        );
        assert!(!entry.source_transfer_started());
    }

    #[test]
    fn source_transfer_started_alpaca_to_base_true_after_initiated() {
        let entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::Initiated,
        );
        assert!(entry.source_transfer_started());
    }

    #[test]
    fn source_transfer_started_alpaca_to_base_true_after_bridged() {
        let entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::Bridged,
        );
        assert!(entry.source_transfer_started());
    }

    #[test]
    fn source_transfer_started_base_to_alpaca_always_true_during_conversion_initiated() {
        let entry = tracking(
            RebalanceDirection::BaseToAlpaca,
            UsdcRebalanceStage::ConversionInitiated,
        );
        assert!(entry.source_transfer_started());
    }

    #[test]
    fn source_transfer_started_base_to_alpaca_always_true_during_conversion_confirmed() {
        let entry = tracking(
            RebalanceDirection::BaseToAlpaca,
            UsdcRebalanceStage::ConversionConfirmed,
        );
        assert!(entry.source_transfer_started());
    }

    #[test]
    fn track_progress_updates_stage_and_timestamp() {
        let mut entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::Initiated,
        );
        entry.track_progress(&withdrawal_confirmed_event());
        assert_eq!(entry.stage, UsdcRebalanceStage::WithdrawalConfirmed);
        assert_eq!(entry.last_progress_at, ts(103));
    }

    #[test]
    fn track_progress_does_not_change_state_when_event_yields_no_stage() {
        let mut entry = tracking(
            RebalanceDirection::AlpacaToBase,
            UsdcRebalanceStage::Initiated,
        );
        entry.track_progress(&withdrawal_failed_event());
        assert_eq!(entry.stage, UsdcRebalanceStage::Initiated);
        assert_eq!(entry.last_progress_at, ts(0));
    }

    #[test]
    fn usdc_rebalancing_check_label_is_static() {
        assert_eq!(
            UsdcRebalancingCheck.label().as_str(),
            "UsdcRebalancingCheck"
        );
    }

    async fn count_pending_usdc_check_jobs(apalis_pool: &apalis_sqlite::SqlitePool) -> i64 {
        let job_type = std::any::type_name::<UsdcRebalancingCheck>();
        sqlx_apalis::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM Jobs WHERE status = 'Pending' AND job_type = ?",
        )
        .bind(job_type)
        .fetch_one(apalis_pool)
        .await
        .expect("count pending usdc-check jobs")
    }

    #[tokio::test]
    async fn usdc_scheduler_enqueue_check_inserts_pending_row() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = UsdcRebalancingCheckScheduler::new(&apalis_pool);

        scheduler.enqueue_check().await;

        assert_eq!(count_pending_usdc_check_jobs(&apalis_pool).await, 1);
    }

    #[tokio::test]
    async fn usdc_scheduler_cancel_pending_marks_pending_rows_done() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = UsdcRebalancingCheckScheduler::new(&apalis_pool);

        scheduler.enqueue_check().await;
        scheduler.enqueue_check().await;
        assert_eq!(count_pending_usdc_check_jobs(&apalis_pool).await, 2);

        scheduler.cancel_pending().await;

        assert_eq!(
            count_pending_usdc_check_jobs(&apalis_pool).await,
            0,
            "cancel_pending must drain all pending rows"
        );
    }

    #[tokio::test]
    async fn usdc_scheduler_enqueue_after_cancel_creates_fresh_row() {
        let apalis_pool = crate::test_utils::setup_test_apalis_pool().await;
        let scheduler = UsdcRebalancingCheckScheduler::new(&apalis_pool);

        scheduler.enqueue_check().await;
        scheduler.cancel_pending().await;
        scheduler.enqueue_check().await;

        assert_eq!(count_pending_usdc_check_jobs(&apalis_pool).await, 1);
    }

    #[test]
    fn track_progress_skips_base_to_alpaca_conversion_confirmed() {
        let mut entry = tracking(
            RebalanceDirection::BaseToAlpaca,
            UsdcRebalanceStage::DepositConfirmed,
        );
        entry.track_progress(&conversion_confirmed_event(
            RebalanceDirection::BaseToAlpaca,
            Usdc::new(float!(50)),
        ));
        assert_eq!(entry.stage, UsdcRebalanceStage::DepositConfirmed);
        assert_eq!(entry.last_progress_at, ts(0));
    }

    #[test]
    fn is_post_burn_is_direction_aware() {
        use RebalanceDirection::{AlpacaToBase, BaseToAlpaca};
        use UsdcRebalanceStage::*;

        let tracking = |direction, stage| UsdcRebalanceTracking {
            direction,
            initiated_amount: Usdc::new(float!(400.0)),
            bridged_amount_received: None,
            stage,
            last_progress_at: ts(0),
        };

        // Stages from BridgingInitiated through DepositConfirmed are post-burn
        // for both directions.
        for stage in [
            BridgingInitiated,
            BridgeAttestationReceived,
            Bridged,
            DepositInitiated,
            DepositConfirmed,
        ] {
            assert!(tracking(AlpacaToBase, stage).is_post_burn(), "A2B {stage}");
            assert!(tracking(BaseToAlpaca, stage).is_post_burn(), "B2A {stage}");
        }

        // Withdrawal stages are pre-burn for both directions.
        for stage in [Initiated, WithdrawalConfirmed] {
            assert!(!tracking(AlpacaToBase, stage).is_post_burn(), "A2B {stage}");
            assert!(!tracking(BaseToAlpaca, stage).is_post_burn(), "B2A {stage}");
        }

        // Conversion stages differ by direction: AlpacaToBase converts before the
        // withdrawal (pre-burn); BaseToAlpaca converts after the deposit (post-mint).
        for stage in [ConversionInitiated, ConversionConfirmed] {
            assert!(
                !tracking(AlpacaToBase, stage).is_post_burn(),
                "A2B {stage} (pre-withdrawal conversion) must be pre-burn"
            );
            assert!(
                tracking(BaseToAlpaca, stage).is_post_burn(),
                "B2A {stage} (post-deposit conversion) must be post-burn"
            );
        }
    }
}
