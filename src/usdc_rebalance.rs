//! Aggregate modeling the lifecycle of cross-chain USDC
//! rebalancing between Alpaca and Base via CCTP.
//!
//! # State Flow
//!
//! The aggregate progresses through states grouped into three phases:
//!
//! ```text
//! WITHDRAWAL PHASE:
//!   Uninitialized --Initiate--> Withdrawing --ConfirmWithdrawal--> WithdrawalComplete
//!                                    |                                     |
//!                                    +--FailWithdrawal--> WithdrawalFailed |
//!                                                                          |
//! BRIDGING PHASE:                                            InitiateBridging
//!                                                                          |
//!                                                          FailBridging    v
//!   Bridging --ReceiveAttestation--> Attested --ConfirmBridging--> Bridged
//!       |              |                 |                             |
//!       |              +--FailBridging---+--> BridgingFailed           |
//!       +--TimeoutAttestation--> AwaitingAttestation                   |
//!                                      |                               |
//!                                      +--ReceiveAttestation--> Attested
//!                                                                      |
//! DEPOSIT PHASE:                                             InitiateDeposit
//!                                                                      |
//!                                                                      v
//!   DepositInitiated --ConfirmDeposit--> DepositConfirmed
//!          |                                    |
//!          +--FailDeposit--> DepositFailed      +--BaseToAlpaca--> ConversionInitiated
//!                                                               |
//!                                                               v
//!                                                   ConversionConfirmed (success)
//!
//! Terminal states:
//! - WithdrawalFailed
//! - BridgingFailed (terminal EXCEPT a post-burn failure carrying a burn tx,
//!   which RecoverBridging un-fails back to Bridged once the mint is confirmed)
//! - DepositFailed
//! - DepositConfirmed for AlpacaToBase
//! - ConversionConfirmed for BaseToAlpaca
//! ```
//!
//! # Direction
//!
//! The aggregate supports bidirectional rebalancing:
//!
//! - **AlpacaToBase**: Withdraw USDC from Alpaca -> CCTP bridge (Ethereum -> Base) ->
//!   Deposit to Rain orderbook vault on Base
//! - **BaseToAlpaca**: Withdraw from Rain vault on Base -> CCTP bridge (Base -> Ethereum) ->
//!   Deposit to Alpaca
//!
//! # Integration Points
//!
//! - **Alpaca API**: Crypto wallet withdrawals and deposits via [`AlpacaWalletService`]
//! - **CCTP Bridge**: Circle's Cross-Chain Transfer Protocol for trustless USDC bridging
//!   - Burn on source chain (Ethereum or Base)
//!   - Attestation from Circle API (~20-30 seconds)
//!   - Mint on destination chain
//! - **Rain Orderbook**: Onchain vault deposits/withdrawals on Base
//!
//! # Error Handling
//!
//! Each phase has dedicated failure states that preserve context for debugging and retry:
//! - `WithdrawalFailed`: Preserves `withdrawal_ref` for tracking the failed transfer
//! - `BridgingFailed`: Preserves `burn_tx_hash` and `cctp_nonce` when available
//! - `DepositFailed`: Preserves `deposit_ref` for tracking the failed deposit
//!
//! Terminal states reject all commands to prevent invalid state transitions,
//! with one exception: a post-burn `BridgingFailed` (carrying a burn tx) accepts
//! `RecoverBridging`, which un-fails it back to `Bridged` once the mint that
//! actually landed is confirmed on-chain.
//!
//! [`AlpacaWalletService`]: st0x_execution::AlpacaWalletService

use alloy::primitives::{B256, TxHash};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use tracing::warn;
use uuid::Uuid;

use st0x_dto::{TransferOperation, UsdcBridgeOperation, UsdcBridgeStatus};
use st0x_event_sorcery::{DomainEvent, EventSourced, JobQueue, Nil};
use st0x_execution::{AlpacaTransferId, ClientOrderId};
use st0x_finance::{HasZero, Id, Usdc};

/// Unique identifier for a USDC rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub(crate) struct UsdcRebalanceId(pub(crate) Uuid);

impl Display for UsdcRebalanceId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for UsdcRebalanceId {
    type Err = uuid::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(Uuid::parse_str(value)?))
    }
}

/// Reference to a transfer, either via Alpaca API or onchain transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TransferRef {
    AlpacaId(AlpacaTransferId),
    OnchainTx(TxHash),
}

/// Direction of the USDC rebalancing operation.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RebalanceDirection {
    AlpacaToBase,
    BaseToAlpaca,
}

impl From<RebalanceDirection> for st0x_dto::UsdcBridgeDirection {
    fn from(direction: RebalanceDirection) -> Self {
        match direction {
            RebalanceDirection::AlpacaToBase => Self::AlpacaToBase,
            RebalanceDirection::BaseToAlpaca => Self::BaseToAlpaca,
        }
    }
}

/// Explicit source and destination cash amounts for a USD <-> USDC conversion.
///
/// Cash amounts remain modeled as [`Usdc`] throughout the system because offchain
/// USD buying power is normalized into the same cash type as onchain USDC.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConversionAmounts {
    pub(crate) source_amount: Usdc,
    pub(crate) received_amount: Usdc,
}

/// Durable Alpaca-to-Base pre-burn state needed to reconstruct the source
/// inventory reservation after a process restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AlpacaToBaseReservationRecovery {
    pub(crate) amount: Usdc,
    pub(crate) stage: AlpacaToBaseReservationStage,
    pub(crate) last_progress_at: DateTime<Utc>,
}

/// The durable lifecycle point represented by a recovered source reservation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AlpacaToBaseReservationStage {
    ConversionComplete,
    Withdrawing,
    WithdrawalComplete,
    BridgingSubmitting,
}

impl ConversionAmounts {
    pub(crate) const fn new(source_amount: Usdc, received_amount: Usdc) -> Self {
        Self {
            source_amount,
            received_amount,
        }
    }
}

/// Why an operator reconciled a USDC rebalance stranded in the post-burn
/// terminal `DepositFailed` state. The funds were handled out-of-band, so the
/// reconcile records the reason rather than re-driving the failed deposit.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum ReconcileReason {
    /// The minted USDC was moved to its destination manually.
    FundsMovedManually,
    /// The deposit was credited at the destination outside the bot's view.
    DepositCreditedOffline,
}

impl Display for ReconcileReason {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FundsMovedManually => formatter.write_str("funds moved manually"),
            Self::DepositCreditedOffline => formatter.write_str("deposit credited offline"),
        }
    }
}

/// Errors that can occur during USDC rebalance operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum UsdcRebalanceError {
    /// Attempted to initiate when already in progress
    #[error("Rebalancing has already been initiated")]
    AlreadyInitiated,
    /// Conversion has not been initiated yet
    #[error("Conversion has not been initiated")]
    ConversionNotInitiated,
    /// Conversion has already completed or failed
    #[error("Conversion has already completed")]
    ConversionAlreadyCompleted,
    /// Deposit has not been confirmed yet (required for post-deposit conversion)
    #[error("Deposit has not been confirmed yet")]
    DepositNotConfirmed,
    /// Wrong direction for post-deposit conversion
    #[error("Post-deposit conversion is only valid for BaseToAlpaca direction")]
    WrongDirectionForPostDepositConversion,
    /// Conversion amount doesn't match aggregate deposit amount
    #[error(
        "Conversion amount mismatch: aggregate has {expected}, \
         command provided {provided}"
    )]
    ConversionAmountMismatch { expected: Usdc, provided: Usdc },
    /// Withdrawal has not been initiated yet
    #[error("Withdrawal has not been initiated")]
    WithdrawalNotInitiated,
    /// Withdrawal has already been confirmed or failed
    #[error("Withdrawal has already completed")]
    WithdrawalAlreadyCompleted,
    /// Withdrawal has not been confirmed yet
    #[error("Withdrawal has not been confirmed")]
    WithdrawalNotConfirmed,

    /// `BeginBridging` carried a burn amount outside the valid actual-burn
    /// range `(0, nominal]`. The manager only emits `0 < burn <= nominal`, so
    /// this rejects a malformed event before it is persisted and trusted by
    /// crash-safe resume.
    #[error("burn amount {burn_amount} is outside the valid range (0, {nominal}]")]
    InvalidBurnAmount { burn_amount: Usdc, nominal: Usdc },
    /// A `BeginBridging` burn amount could not be compared against the nominal
    /// amount because the underlying `Float` comparison errored. We fail closed
    /// -- rejecting the event rather than letting `Usdc`'s `PartialOrd` collapse
    /// the error to a silent "in range" -- so an un-comparable burn amount is
    /// never persisted and trusted by crash-safe resume. `FloatError` is not
    /// carried because this error must stay `Serialize`/`Eq` for the aggregate.
    #[error("burn amount {burn_amount} could not be compared against nominal {nominal}")]
    BurnAmountUncomparable { burn_amount: Usdc, nominal: Usdc },
    /// Bridging has not been initiated yet
    #[error("Bridging has not been initiated")]
    BridgingNotInitiated,
    /// Attestation has not been received yet
    #[error("Attestation has not been received")]
    AttestationNotReceived,
    /// Bridging has already completed or failed
    #[error("Bridging has already completed")]
    BridgingAlreadyCompleted,
    /// A CCTP burn tx hash was durably recorded (`pending_burn_tx`), so a burn may
    /// already be on-chain. Failing as pre-burn (`burn_tx_hash: None`) would clear
    /// the double-burn guard and strand the funds; the operator must instead resume
    /// (to adopt/await the recorded burn) or verify on-chain.
    #[error(
        "a CCTP burn tx was already recorded for this transfer; cannot fail it as \
         pre-burn -- resume to adopt/await the recorded burn"
    )]
    BurnAlreadyRecorded,
    /// A pending-burn command (`RecordPendingBurn` / `ClearPendingBurn`) arrived
    /// after bridging already advanced past `BridgingSubmitting`. The burn is
    /// settled and the pending-burn slot no longer exists, so there is nothing to
    /// record or clear.
    #[error(
        "bridging already advanced past BridgingSubmitting; cannot record or clear \
         a pending burn"
    )]
    BridgingAlreadyStarted,
    /// Bridging has not been completed yet
    #[error("Bridging has not been completed")]
    BridgingNotCompleted,
    /// Deposit has not been initiated yet
    #[error("Deposit has not been initiated")]
    DepositNotInitiated,
    /// Command not valid for current state
    #[error("Command {command} not valid for state {state}")]
    InvalidCommand { command: String, state: String },
}

/// Commands for the USDC rebalance aggregate.
///
/// Commands are validated against the current state before being processed.
/// Invalid commands return appropriate errors without mutating state.
///
/// # Conversion Commands
///
/// There are two conversion commands because conversion happens at different points in each flow:
/// - **AlpacaToBase**: Convert USD->USDC BEFORE withdrawal (need USDC for CCTP bridge)
/// - **BaseToAlpaca**: Convert USDC->USD AFTER deposit (USDC arrives in crypto wallet)
#[derive(Debug, Clone)]
pub(crate) enum UsdcRebalanceCommand {
    /// Start pre-withdrawal conversion for AlpacaToBase direction.
    /// Converts USD buying power to USDC before withdrawal to Alpaca's crypto wallet.
    /// Valid only from `Uninitialized` state.
    InitiateConversion {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: ClientOrderId,
    },
    /// Test/fixture-only: identical to `InitiateConversion` but takes
    /// `initiated_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    InitiateConversionAt {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: ClientOrderId,
        initiated_at: DateTime<Utc>,
    },
    /// Confirm successful conversion. Valid only from `Converting` state.
    /// Contains the source amount sold and the destination amount received.
    ConfirmConversion { conversion: ConversionAmounts },
    /// Test/fixture-only: identical to `ConfirmConversion` but takes
    /// `converted_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    ConfirmConversionAt {
        conversion: ConversionAmounts,
        converted_at: DateTime<Utc>,
    },
    /// Record conversion failure. Valid only from `Converting` state.
    FailConversion { reason: String },
    /// Start post-deposit conversion for BaseToAlpaca direction.
    /// Converts USDC (deposited via CCTP) to USD buying power for trading.
    /// Valid only from `DepositConfirmed` state.
    /// Amount must match the aggregate's deposit amount for consistency.
    InitiatePostDepositConversion {
        order_id: ClientOrderId,
        amount: Usdc,
    },
    /// Test/fixture-only: identical to `InitiatePostDepositConversion` but
    /// takes `initiated_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    InitiatePostDepositConversionAt {
        order_id: ClientOrderId,
        amount: Usdc,
        initiated_at: DateTime<Utc>,
    },
    /// Record the intent to withdraw from source BEFORE the on-chain call.
    /// Captures the chain head (`from_block`) so a crash mid-withdrawal can be
    /// recovered by scanning from that block instead of blindly re-withdrawing
    /// (which would double-spend). Valid from `Uninitialized` (BaseToAlpaca) or
    /// `ConversionComplete` (AlpacaToBase).
    BeginWithdrawal {
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
    },
    /// Test/fixture-only: identical to `BeginWithdrawal` but takes
    /// `submitting_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    BeginWithdrawalAt {
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        submitting_at: DateTime<Utc>,
    },
    /// Start a new rebalancing operation by recording the submitted withdrawal
    /// transaction. Valid only from `WithdrawalSubmitting` state.
    Initiate {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
    },
    /// Test/fixture-only: identical to `Initiate` but takes `initiated_at`
    /// explicitly instead of stamping `Utc::now()`, so fixture seeding can
    /// backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    InitiateAt {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
        initiated_at: DateTime<Utc>,
    },
    /// Confirm successful withdrawal from source. Valid only from `Withdrawing` state.
    /// `withdrawal_tx` carries the on-chain tx hash when the source provides one
    /// (AlpacaToBase), so the confirmation-depth gate can re-run on apalis redrive.
    ConfirmWithdrawal { withdrawal_tx: Option<TxHash> },
    /// Test/fixture-only: identical to `ConfirmWithdrawal` but takes
    /// `confirmed_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    ConfirmWithdrawalAt {
        withdrawal_tx: Option<TxHash>,
        confirmed_at: DateTime<Utc>,
    },
    /// Record the intent to burn for CCTP bridging BEFORE the on-chain call.
    /// Captures the chain head (`from_block`) for crash recovery, mirroring
    /// [`BeginWithdrawal`]. Valid only from `WithdrawalComplete` state.
    BeginBridging {
        from_block: u64,
        burn_amount: Option<Usdc>,
    },
    /// Test/fixture-only: identical to `BeginBridging` but takes
    /// `submitting_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    BeginBridgingAt {
        from_block: u64,
        burn_amount: Option<Usdc>,
        submitting_at: DateTime<Utc>,
    },
    /// Record the CCTP burn transaction. Valid only from `BridgingSubmitting` state.
    InitiateBridging { burn_tx: TxHash },
    /// Test/fixture-only: identical to `InitiateBridging` but takes
    /// `burned_at` explicitly instead of stamping `Utc::now()`, so fixture
    /// seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    InitiateBridgingAt {
        burn_tx: TxHash,
        burned_at: DateTime<Utc>,
    },
    /// Record the broadcast CCTP burn tx hash while still in `BridgingSubmitting`,
    /// BEFORE awaiting its receipt, so a crash/timeout redrive checks that exact
    /// tx instead of a mempool-blind log scan. Valid only from `BridgingSubmitting`.
    RecordPendingBurn { burn_tx: TxHash },
    /// Clear any durably-recorded pending burn tx, returning `BridgingSubmitting` to
    /// `pending_burn_tx: None`. Emitted before a (re)broadcast so that if recording the
    /// new burn's hash fails, the resume path sees None and fails closed instead of
    /// reburning off a stale (reverted) hash. Valid only from `BridgingSubmitting`.
    ClearPendingBurn,
    /// Record the Circle attestation. Valid from `Bridging` or
    /// `AwaitingAttestation` state.
    /// The cctp_nonce is extracted from the attested message (not the burn tx, which has placeholder).
    /// `mint_scan_from_block` is the destination chain head captured before the mint,
    /// bounding the crash-safe resume scan that adopts an already-submitted mint.
    ReceiveAttestation {
        attestation: Vec<u8>,
        cctp_nonce: B256,
        /// Full CCTP message envelope from the attestation, persisted on the
        /// `BridgeAttestationReceived` event so an `Attested` resume mints
        /// without re-polling Circle.
        message: Vec<u8>,
        mint_scan_from_block: u64,
    },
    /// Test/fixture-only: identical to `ReceiveAttestation` but takes
    /// `attested_at` explicitly instead of stamping `Utc::now()`, so
    /// fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    ReceiveAttestationAt {
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Vec<u8>,
        mint_scan_from_block: u64,
        attested_at: DateTime<Utc>,
    },
    /// Record that Circle attestation polling timed out while the burn remains
    /// recoverable. Valid only from `Bridging` state.
    TimeoutAttestation { retry_deadline_at: DateTime<Utc> },
    /// Confirm the CCTP mint transaction. Valid only from `Attested` state.
    /// Includes actual amounts from the MintAndWithdraw event for accurate inventory tracking.
    ConfirmBridging {
        mint_tx: TxHash,
        /// Actual USDC received (from MintAndWithdraw event)
        amount_received: Usdc,
        /// CCTP fee collected (from MintAndWithdraw event)
        fee_collected: Usdc,
    },
    /// Test/fixture-only: identical to `ConfirmBridging` but takes
    /// `minted_at` explicitly instead of stamping `Utc::now()`, so fixture
    /// seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    ConfirmBridgingAt {
        mint_tx: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
        minted_at: DateTime<Utc>,
    },
    /// Start deposit to destination. Valid only from `Bridged` state.
    InitiateDeposit { deposit: TransferRef },
    /// Test/fixture-only: identical to `InitiateDeposit` but takes
    /// `deposit_initiated_at` explicitly instead of stamping `Utc::now()`,
    /// so fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    InitiateDepositAt {
        deposit: TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    },
    /// Confirm successful deposit. Valid only from `DepositInitiated` state.
    ConfirmDeposit,
    /// Test/fixture-only: identical to `ConfirmDeposit` but takes
    /// `deposit_confirmed_at` explicitly instead of stamping `Utc::now()`,
    /// so fixture seeding can backdate synthetic history.
    #[cfg(any(test, feature = "test-support"))]
    ConfirmDepositAt { deposit_confirmed_at: DateTime<Utc> },
    /// Record withdrawal failure. Valid only from `Withdrawing` state.
    FailWithdrawal { reason: String },
    /// Record bridging failure. Valid from `Bridging` or `Attested` states.
    FailBridging { reason: String },
    /// Recover a post-burn `BridgingFailed` whose CCTP mint actually landed
    /// on-chain (a transient receipt error failed the bridge while the
    /// `receiveMessage` in fact succeeded). Transitions back to `Bridged` so the
    /// deposit leg completes and the in-progress guard clears via the normal
    /// terminal path. Valid ONLY from a `BridgingFailed` carrying a burn tx
    /// (post-burn). Driven by a resume/recheck of the failed transfer.
    RecoverBridging {
        mint_tx: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
    },
    /// Record deposit failure. Valid only from `DepositInitiated` state.
    FailDeposit { reason: String },
    /// Reconcile a USDC rebalance stranded in the post-burn terminal
    /// `DepositFailed` state to a guard-clearing terminal `Reconciled` state.
    /// The minted USDC was handled out-of-band, so this resolves the transfer
    /// (clearing the in-progress guard and reconciling source-venue inflight)
    /// rather than re-driving the Alpaca deposit. Valid ONLY from
    /// `DepositFailed`.
    ReconcileStuckRebalance { reason: ReconcileReason },
}

/// Events emitted by the USDC rebalance aggregate.
///
/// Events represent immutable facts that have occurred and are persisted to the event store.
/// Each event carries only the data relevant to that state transition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UsdcRebalanceEvent {
    /// Conversion operation started (USD<->USDC). Records direction, amount, and order ID.
    ConversionInitiated {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: ClientOrderId,
        initiated_at: DateTime<Utc>,
    },
    /// Conversion completed successfully.
    /// Includes direction so terminal detection works with incremental dispatch
    /// (cqrs-es Query::dispatch only receives newly committed events, not full history).
    /// Captures both the source amount sold and the destination amount received.
    ConversionConfirmed {
        direction: RebalanceDirection,
        #[serde(flatten)]
        conversion: ConversionAmounts,
        converted_at: DateTime<Utc>,
    },
    /// Conversion failed.
    ConversionFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// Intent to withdraw from source recorded before the on-chain call.
    /// Captures the chain head for crash-safe recovery.
    WithdrawalSubmitting {
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        submitting_at: DateTime<Utc>,
    },
    /// Rebalancing operation started. Records direction, amount, and withdrawal reference.
    Initiated {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
    /// Withdrawal from source completed successfully.
    WithdrawalConfirmed {
        confirmed_at: DateTime<Utc>,
        /// On-chain tx hash that delivered USDC to the market-maker wallet.
        /// `None` if the withdrawal source did not return a tx hash (e.g. onchain
        /// vault withdrawal in BaseToAlpaca). `#[serde(default)]` ensures
        /// backward-compatibility with events persisted before this field existed.
        #[serde(default)]
        withdrawal_tx: Option<TxHash>,
    },
    /// Withdrawal from source failed.
    WithdrawalFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// Intent to burn for CCTP bridging recorded before the on-chain call.
    /// Captures the chain head for crash-safe recovery.
    BridgingSubmitting {
        from_block: u64,
        submitting_at: DateTime<Utc>,
        /// Actual burn amount (received after Alpaca withdrawal fees).
        /// `None` for events persisted before this field existed.
        #[serde(default)]
        burn_amount: Option<Usdc>,
    },
    /// CCTP burn transaction submitted. Records burn hash for attestation lookup.
    BridgingInitiated {
        burn_tx_hash: TxHash,
        burned_at: DateTime<Utc>,
    },
    /// A CCTP burn tx hash was broadcast and recorded while still in
    /// `BridgingSubmitting`, before its receipt was awaited. Lets a crash/timeout
    /// redrive check that exact tx instead of a mempool-blind log scan.
    PendingBurnRecorded {
        burn_tx: TxHash,
        recorded_at: DateTime<Utc>,
    },
    /// The recorded pending burn tx was cleared (back to `pending_burn_tx: None`)
    /// before a (re)broadcast, so a failed re-record fails closed rather than reburning.
    PendingBurnCleared { cleared_at: DateTime<Utc> },
    /// Circle attestation received. Enables minting on destination chain.
    /// The cctp_nonce is extracted from the attested message (the real nonce, not the placeholder).
    /// `mint_scan_from_block` is the destination chain head captured before the mint,
    /// persisted so a crash-safe resume scan for an already-submitted mint is bounded.
    /// It is `None` for events persisted before this field existed: such a resume
    /// has no bound and must reconcile manually rather than scan from genesis,
    /// which could adopt an unrelated mint to the same wallet.
    BridgeAttestationReceived {
        attestation: Vec<u8>,
        cctp_nonce: B256,
        /// Full CCTP message envelope, persisted so an `Attested` resume mints
        /// without re-polling Circle. `None` for events serialized before this
        /// field existed: such a resume re-polls Circle as a fallback.
        #[serde(default)]
        message: Option<Vec<u8>>,
        #[serde(default)]
        mint_scan_from_block: Option<u64>,
        attested_at: DateTime<Utc>,
    },
    /// Circle attestation polling timed out, but the burn remains recoverable
    /// until the retry deadline.
    AttestationTimedOut {
        burn_tx_hash: TxHash,
        retry_deadline_at: DateTime<Utc>,
        timed_out_at: DateTime<Utc>,
    },
    /// CCTP mint transaction confirmed on destination chain.
    /// Contains actual amount received (after CCTP fees) for accurate inventory tracking.
    Bridged {
        mint_tx_hash: TxHash,
        /// Actual USDC received on destination chain (requested amount - CCTP fee)
        amount_received: Usdc,
        /// CCTP fee collected during the bridge
        fee_collected: Usdc,
        minted_at: DateTime<Utc>,
    },
    /// Bridging failed. Preserves burn data when available for debugging.
    BridgingFailed {
        burn_tx_hash: Option<TxHash>,
        cctp_nonce: Option<B256>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// A post-burn `BridgingFailed` whose CCTP mint was confirmed on-chain after
    /// the fact. Un-fails the aggregate back to `Bridged` so the deposit leg can
    /// finish. Mirrors equities' `ProviderCompletionRecovered`.
    BridgingCompletionRecovered {
        mint_tx_hash: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
        recovered_at: DateTime<Utc>,
    },
    /// Deposit to destination initiated.
    DepositInitiated {
        deposit_ref: TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    },
    /// Deposit completed successfully. Terminal success state for AlpacaToBase.
    /// For BaseToAlpaca, post-deposit conversion (USDC->USD) still required.
    DepositConfirmed {
        direction: RebalanceDirection,
        deposit_confirmed_at: DateTime<Utc>,
    },
    /// Deposit failed. Preserves deposit reference when available.
    DepositFailed {
        deposit_ref: Option<TransferRef>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// An operator reconciled a post-burn `DepositFailed` rebalance to the
    /// guard-clearing terminal `Reconciled` state. Carries `direction` so the
    /// reactor derives the source venue without in-memory tracking (which may
    /// be absent after a restart), plus `amount` and `initiated_at` so the
    /// projection preserves the real post-burn transfer instead of defaulting
    /// it to a zero-value transfer starting at reconciliation time.
    ///
    /// `failure_reason` is intentionally absent from the event (unlike the
    /// first-pass design): `apply()` derives it from the prior `Failed` state
    /// so historical events (persisted before the field was introduced) are
    /// recovered correctly on replay without a serde-default migration.
    OperatorReconciled {
        direction: RebalanceDirection,
        amount: Usdc,
        reason: ReconcileReason,
        initiated_at: DateTime<Utc>,
        reconciled_at: DateTime<Utc>,
    },
}

impl DomainEvent for UsdcRebalanceEvent {
    fn event_type(&self) -> String {
        match self {
            Self::ConversionInitiated { .. } => "UsdcRebalanceEvent::ConversionInitiated",
            Self::ConversionConfirmed { .. } => "UsdcRebalanceEvent::ConversionConfirmed",
            Self::ConversionFailed { .. } => "UsdcRebalanceEvent::ConversionFailed",
            Self::WithdrawalSubmitting { .. } => "UsdcRebalanceEvent::WithdrawalSubmitting",
            Self::Initiated { .. } => "UsdcRebalanceEvent::Initiated",
            Self::WithdrawalConfirmed { .. } => "UsdcRebalanceEvent::WithdrawalConfirmed",
            Self::WithdrawalFailed { .. } => "UsdcRebalanceEvent::WithdrawalFailed",
            Self::BridgingSubmitting { .. } => "UsdcRebalanceEvent::BridgingSubmitting",
            Self::BridgingInitiated { .. } => "UsdcRebalanceEvent::BridgingInitiated",
            Self::PendingBurnRecorded { .. } => "UsdcRebalanceEvent::PendingBurnRecorded",
            Self::PendingBurnCleared { .. } => "UsdcRebalanceEvent::PendingBurnCleared",
            Self::BridgeAttestationReceived { .. } => {
                "UsdcRebalanceEvent::BridgeAttestationReceived"
            }
            Self::AttestationTimedOut { .. } => "UsdcRebalanceEvent::AttestationTimedOut",
            Self::Bridged { .. } => "UsdcRebalanceEvent::Bridged",
            Self::BridgingFailed { .. } => "UsdcRebalanceEvent::BridgingFailed",
            Self::BridgingCompletionRecovered { .. } => {
                "UsdcRebalanceEvent::BridgingCompletionRecovered"
            }
            Self::DepositInitiated { .. } => "UsdcRebalanceEvent::DepositInitiated",
            Self::DepositConfirmed { .. } => "UsdcRebalanceEvent::DepositConfirmed",
            Self::DepositFailed { .. } => "UsdcRebalanceEvent::DepositFailed",
            Self::OperatorReconciled { .. } => "UsdcRebalanceEvent::OperatorReconciled",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "2.0".to_string()
    }
}

/// USDC rebalance aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid states unrepresentable.
/// Each variant contains exactly the data valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum UsdcRebalance {
    /// USD/USDC conversion has been initiated (AlpacaToBase: USD->USDC, BaseToAlpaca: USDC->USD)
    Converting {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: ClientOrderId,
        initiated_at: DateTime<Utc>,
    },
    /// Conversion has completed, ready for next phase
    ConversionComplete {
        direction: RebalanceDirection,
        /// Originally requested amount
        amount: Usdc,
        conversion: ConversionAmounts,
        initiated_at: DateTime<Utc>,
        converted_at: DateTime<Utc>,
    },
    /// Conversion has failed (terminal state)
    ConversionFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: ClientOrderId,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// Withdrawal intent recorded; the on-chain withdraw is about to be (or has
    /// just been) submitted. `from_block` is the chain head captured before the
    /// call so resume can scan for an already-submitted withdrawal.
    WithdrawalSubmitting {
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        initiated_at: DateTime<Utc>,
    },
    /// Withdrawal from source has been initiated
    Withdrawing {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        initiated_at: DateTime<Utc>,
    },
    /// Withdrawal from source has been confirmed, ready for bridging
    WithdrawalComplete {
        direction: RebalanceDirection,
        amount: Usdc,
        initiated_at: DateTime<Utc>,
        confirmed_at: DateTime<Utc>,
        /// Persisted so the confirmation-depth gate can re-run on apalis redrive.
        /// `#[serde(default)]` ensures backward-compat with snapshots persisted
        /// before this field was added.
        #[serde(default)]
        withdrawal_tx: Option<TxHash>,
    },
    /// Withdrawal from source has failed (terminal state)
    WithdrawalFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal_ref: TransferRef,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// Bridging intent recorded; the on-chain CCTP burn is about to be (or has
    /// just been) submitted. `from_block` is the chain head captured before the
    /// call so resume can scan for an already-submitted burn.
    BridgingSubmitting {
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        initiated_at: DateTime<Utc>,
        /// Actual burn amount (received after Alpaca withdrawal fees).
        /// `None` for aggregates persisted before this field existed.
        #[serde(default)]
        burn_amount: Option<Usdc>,
        /// CCTP burn tx hash broadcast before its receipt was awaited, recorded
        /// via `RecordPendingBurn`. A crash/timeout redrive checks this exact tx
        /// (`burn_status`) instead of a mempool-blind log scan, closing the
        /// double-burn window. `None` until the burn is broadcast, and for
        /// aggregates persisted before this field existed.
        #[serde(default)]
        pending_burn_tx: Option<TxHash>,
    },
    /// CCTP bridging has been initiated (burn transaction submitted)
    /// Note: cctp_nonce is not available here - it's only known after attestation.
    Bridging {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        initiated_at: DateTime<Utc>,
        burned_at: DateTime<Utc>,
    },
    /// CCTP burn has succeeded and Circle attestation has not arrived within
    /// the per-poll timeout. The burn remains recoverable, so the aggregate is
    /// non-terminal and the apalis transfer job can retry until the deadline.
    AwaitingAttestation {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        initiated_at: DateTime<Utc>,
        timed_out_at: DateTime<Utc>,
        retry_deadline_at: DateTime<Utc>,
    },
    /// Circle attestation has been received, ready for minting on destination chain
    Attested {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        cctp_nonce: B256,
        attestation: Vec<u8>,
        /// Full CCTP message envelope from the attestation, persisted so a resume
        /// can reconstruct the `AttestationResponse` and mint without re-polling
        /// Circle. `None` for transfers whose `BridgeAttestationReceived` predates
        /// this field: such a resume falls back to re-polling Circle.
        message: Option<Vec<u8>>,
        /// Destination chain head captured before the mint, bounding the
        /// crash-safe resume scan that adopts an already-submitted mint. `None`
        /// for transfers whose `BridgeAttestationReceived` predates this field.
        mint_scan_from_block: Option<u64>,
        initiated_at: DateTime<Utc>,
        attested_at: DateTime<Utc>,
    },
    /// USDC has been minted on destination chain via CCTP
    Bridged {
        direction: RebalanceDirection,
        /// Originally requested amount (before CCTP fee)
        amount: Usdc,
        /// Actual USDC received on destination chain (from MintAndWithdraw event)
        amount_received: Usdc,
        /// CCTP fee collected during the bridge (from MintAndWithdraw event)
        fee_collected: Usdc,
        burn_tx_hash: TxHash,
        mint_tx_hash: TxHash,
        initiated_at: DateTime<Utc>,
        minted_at: DateTime<Utc>,
    },
    /// Bridging has failed (terminal state)
    BridgingFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: Option<TxHash>,
        cctp_nonce: Option<B256>,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// Deposit to destination has been initiated
    DepositInitiated {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        mint_tx_hash: TxHash,
        deposit_ref: TransferRef,
        initiated_at: DateTime<Utc>,
        deposit_initiated_at: DateTime<Utc>,
    },
    /// Deposit has been confirmed (terminal state)
    DepositConfirmed {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        mint_tx_hash: TxHash,
        initiated_at: DateTime<Utc>,
        deposit_confirmed_at: DateTime<Utc>,
    },
    /// Deposit has failed (terminal state)
    DepositFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        mint_tx_hash: TxHash,
        deposit_ref: Option<TransferRef>,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
    /// An operator reconciled a post-burn `DepositFailed` rebalance
    /// out-of-band (clearable terminal state). The in-progress guard clears and
    /// source-venue inflight is reconciled with post-burn semantics. Retains
    /// `amount` and `initiated_at` so the projection still reports the real
    /// transfer rather than a zero-value one starting at `reconciled_at`.
    Reconciled {
        direction: RebalanceDirection,
        amount: Usdc,
        reason: ReconcileReason,
        /// The original failure message from the source terminal state.
        /// `None` for aggregates reconciled before this field was added.
        #[serde(default)]
        failure_reason: Option<String>,
        initiated_at: DateTime<Utc>,
        reconciled_at: DateTime<Utc>,
    },
}

impl UsdcRebalance {
    /// Whether a failed rebalance failed POST-burn -- the only USDC failures
    /// `transfer reconcile --kind usdc` accepts (a pre-burn failure strands
    /// nothing and the CLI rejects it). MUST stay in sync with the reconcilable
    /// set in `transition_reconcile_stuck_rebalance`. A new (non-failure or
    /// future) state defaults to `false`, i.e. not reconcilable -- the safe
    /// default for surfacing the command on the dashboard.
    fn is_post_burn_failure(&self) -> bool {
        match self {
            Self::DepositFailed { .. }
            | Self::ConversionFailed {
                direction: RebalanceDirection::BaseToAlpaca,
                ..
            } => true,
            Self::BridgingFailed {
                burn_tx_hash,
                cctp_nonce,
                ..
            } => burn_tx_hash.is_some() || cctp_nonce.is_some(),
            // Everything else is not a post-burn failure: pre-burn failures
            // strand nothing on-chain (WithdrawalFailed, and an AlpacaToBase
            // ConversionFailed which is pre-withdrawal), and in-flight or
            // clearable-terminal states (success / Reconciled) are not failures
            // at all -- so the CLI rejects `transfer reconcile --kind usdc` for
            // all of them. Enumerated exhaustively rather than via a wildcard so
            // a new variant forces a conscious post-burn classification here
            // instead of silently defaulting to false and diverging from the
            // reconcilable set in `transition_reconcile_stuck_rebalance`.
            Self::WithdrawalFailed { .. }
            | Self::ConversionFailed {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            }
            | Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::Reconciled { .. } => false,
        }
    }

    // A single state-dispatch match mapping each state to its DTO tuple, one
    // trivial arm per state. Splitting it would scatter the mapping across
    // pointless helpers; per the project guidance, suppress the length lint.
    #[expect(clippy::too_many_lines)]
    pub(crate) fn to_dto(&self, id: &UsdcRebalanceId) -> TransferOperation {
        let (direction, amount, status, started_at, updated_at) = match self {
            Self::Converting {
                direction,
                amount,
                initiated_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Converting,
                *initiated_at,
                *initiated_at,
            ),

            Self::ConversionComplete {
                direction,
                conversion,
                initiated_at,
                converted_at,
                ..
            } => {
                let status = match direction {
                    // Pre-withdrawal conversion complete, still has withdrawal/bridging/deposit ahead
                    RebalanceDirection::AlpacaToBase => UsdcBridgeStatus::Converting,
                    // Post-deposit conversion complete -> truly done
                    RebalanceDirection::BaseToAlpaca => UsdcBridgeStatus::Completed {
                        completed_at: *converted_at,
                    },
                };

                (
                    direction,
                    conversion.received_amount,
                    status,
                    *initiated_at,
                    *converted_at,
                )
            }

            Self::ConversionFailed {
                direction,
                amount,
                initiated_at,
                failed_at,
                ..
            }
            | Self::WithdrawalFailed {
                direction,
                amount,
                initiated_at,
                failed_at,
                ..
            }
            | Self::BridgingFailed {
                direction,
                amount,
                initiated_at,
                failed_at,
                ..
            }
            | Self::DepositFailed {
                direction,
                amount,
                initiated_at,
                failed_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Failed {
                    failed_at: *failed_at,
                    post_burn: self.is_post_burn_failure(),
                },
                *initiated_at,
                *failed_at,
            ),

            Self::WithdrawalSubmitting {
                direction,
                amount,
                initiated_at,
                ..
            }
            | Self::Withdrawing {
                direction,
                amount,
                initiated_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Withdrawing,
                *initiated_at,
                *initiated_at,
            ),

            Self::WithdrawalComplete {
                direction,
                amount,
                initiated_at,
                confirmed_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Withdrawing,
                *initiated_at,
                *confirmed_at,
            ),

            Self::BridgingSubmitting {
                direction,
                amount,
                initiated_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Bridging,
                *initiated_at,
                *initiated_at,
            ),

            Self::Bridging {
                direction,
                amount,
                initiated_at,
                burned_at: updated_at,
                ..
            }
            | Self::AwaitingAttestation {
                direction,
                amount,
                initiated_at,
                timed_out_at: updated_at,
                ..
            }
            | Self::Attested {
                direction,
                amount,
                initiated_at,
                attested_at: updated_at,
                ..
            }
            | Self::Bridged {
                direction,
                amount_received: amount,
                initiated_at,
                minted_at: updated_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Bridging,
                *initiated_at,
                *updated_at,
            ),

            Self::DepositInitiated {
                direction,
                amount,
                initiated_at,
                deposit_initiated_at,
                ..
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Depositing,
                *initiated_at,
                *deposit_initiated_at,
            ),

            Self::DepositConfirmed {
                direction,
                amount,
                initiated_at,
                deposit_confirmed_at,
                ..
            } => {
                let status = match direction {
                    // Deposit confirmed is terminal for AlpacaToBase
                    RebalanceDirection::AlpacaToBase => UsdcBridgeStatus::Completed {
                        completed_at: *deposit_confirmed_at,
                    },
                    // BaseToAlpaca still needs post-deposit USDC->USD conversion
                    RebalanceDirection::BaseToAlpaca => UsdcBridgeStatus::Converting,
                };

                (
                    direction,
                    *amount,
                    status,
                    *initiated_at,
                    *deposit_confirmed_at,
                )
            }

            Self::Reconciled {
                direction,
                amount,
                reason,
                failure_reason,
                initiated_at,
                reconciled_at,
            } => (
                direction,
                *amount,
                UsdcBridgeStatus::Reconciled {
                    reconciled_at: *reconciled_at,
                    failure_reason: failure_reason.clone(),
                    reconcile_reason: reason.to_string(),
                },
                *initiated_at,
                *reconciled_at,
            ),
        };

        TransferOperation::UsdcBridge(UsdcBridgeOperation {
            id: Id::new(id.to_string()),
            direction: match direction {
                RebalanceDirection::AlpacaToBase => st0x_dto::UsdcBridgeDirection::AlpacaToBase,
                RebalanceDirection::BaseToAlpaca => st0x_dto::UsdcBridgeDirection::BaseToAlpaca,
            },
            amount,
            status,
            started_at,
            updated_at,
        })
    }

    /// Whether an aggregate in this state should hold the single-rebalance
    /// guard (`usdc_in_progress`) when the guard is reconstructed on startup.
    ///
    /// True for any state where a rebalance is still in progress or stranded
    /// after a CCTP burn; false only for clearable-terminal states -- success,
    /// or a failure that reconciles inflight back to the source venue. The live
    /// reactor holds the guard from dispatch until a clearable-terminal event;
    /// re-deriving that boolean from durable event state keeps a restart from
    /// re-opening the re-burn window. See ADR 0003.
    pub(crate) fn holds_rebalance_guard(&self) -> bool {
        match self {
            // `WithdrawalSubmitting`/`BridgingSubmitting` are transient intent
            // markers persisted immediately before an irreversible on-chain
            // withdraw/burn. A crash here may have already broadcast that effect,
            // so they hold the guard defensively to keep a restart from opening a
            // re-burn window while apalis resumes the job.
            Self::Converting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::DepositInitiated { .. }
            // DepositFailed is post-burn/post-mint (the CCTP mint already
            // happened), so the funds cannot be reconciled to source; the live
            // reactor preserves it (PreservePostBurn), so recovery holds too.
            | Self::DepositFailed { .. } => true,
            // AlpacaToBase: pre-withdrawal conversion, still has withdraw/bridge/
            // deposit ahead. BaseToAlpaca: post-deposit conversion -> terminal.
            Self::ConversionComplete { direction, .. } => match direction {
                RebalanceDirection::AlpacaToBase => true,
                RebalanceDirection::BaseToAlpaca => false,
            },
            // Post-burn failure (burn already broadcast) keeps the guard so a
            // re-burn cannot fire against funds CCTP already burned. `FailBridging`
            // from post-burn `Bridging`/`Attested` states emits `burn_tx_hash: Some`
            // and holds the guard. A plain pre-burn `FailBridging` from
            // `WithdrawalComplete` carries `burn_tx_hash: None` and reconciles to
            // source.
            //
            // For a BaseToAlpaca post-burn failure this is not a permanent latch:
            // it is now recoverable and auto-re-armed on startup, and recovery
            // drives `BridgingFailed -> Bridged -> deposit -> terminal`, at which
            // point this returns `false` and the guard clears via the normal
            // terminal path with no operator action. An AlpacaToBase post-burn
            // failure also holds the guard here, but has no recovery path yet
            // (neither startup re-arm nor the resume CLI drive it), so it stays
            // held until manual reconciliation.
            Self::BridgingFailed { burn_tx_hash, .. } => burn_tx_hash.is_some(),
            // BaseToAlpaca-only post-burn legs: after DepositConfirmed the
            // post-deposit USDC->USD conversion is still pending, and a
            // post-deposit ConversionFailed is post-mint -- both hold. For
            // AlpacaToBase both clear: DepositConfirmed is terminal success and
            // ConversionFailed is the pre-withdrawal (pre-burn) leg.
            Self::DepositConfirmed { direction, .. } | Self::ConversionFailed { direction, .. } => {
                match direction {
                    RebalanceDirection::BaseToAlpaca => true,
                    RebalanceDirection::AlpacaToBase => false,
                }
            }
            // Withdrawal failure is always pre-burn: reconcile to source and
            // clear. Operator reconciliation is the explicit clearing terminal
            // for a post-burn `DepositFailed`, so the guard must not re-latch on
            // startup for it either.
            Self::WithdrawalFailed { .. } | Self::Reconciled { .. } => false,
        }
    }

    /// The `(direction, amount)` of a post-burn transfer that an apalis job must
    /// drive forward, or `None` if the state needs no job (terminal, or a
    /// pre-burn leg the live reactor re-arms on its own).
    ///
    /// Used by startup recovery to re-enqueue a transfer job for an aggregate
    /// stranded post-burn with no pending job -- e.g. a redrive enqueue that
    /// failed after `TimeoutAttestation` committed, leaving the aggregate durably
    /// `AwaitingAttestation` with nothing to retry it. Scoped to the post-burn,
    /// pre-mint-confirmation states (`Bridging`, `AwaitingAttestation`,
    /// `Attested`) where polling/minting is the only thing advancing the transfer
    /// and a lost job latches the guard indefinitely.
    pub(crate) fn resumable_post_burn_transfer(&self) -> Option<(RebalanceDirection, Usdc)> {
        match self {
            // The post-burn, pre-mint-confirmation states a job must keep
            // driving, PLUS a post-burn `BridgingFailed`, which is recoverable
            // (RAI-909): the BaseToAlpaca resume path re-checks the mint
            // on-chain, un-fails the aggregate, and finishes the deposit. Re-arm
            // it on startup so a transfer stranded by a transient mint-receipt
            // error (the RAI-903 incident) self-heals without operator action
            // (RAI-906). The `BridgingFailed` arm is scoped to BaseToAlpaca with
            // a burn tx: a pre-burn failure (`burn_tx_hash` `None`) has no mint
            // to adopt, and the AlpacaToBase recovery path is not yet
            // implemented -- both are left for manual reconciliation.
            Self::Bridging {
                direction, amount, ..
            }
            | Self::AwaitingAttestation {
                direction, amount, ..
            }
            | Self::Attested {
                direction, amount, ..
            }
            | Self::BridgingFailed {
                direction: direction @ RebalanceDirection::BaseToAlpaca,
                amount,
                burn_tx_hash: Some(_),
                ..
            } => Some((*direction, *amount)),
            _ => None,
        }
    }

    /// The rebalance direction, present in every non-initial state and invariant
    /// across the whole lifecycle. Used by the CLI `transfer resume --kind usdc`
    /// command to validate the operator-supplied `--direction` against the persisted
    /// transfer, so a wrong flag is rejected rather than mis-driving the aggregate
    /// through the opposite-direction resume path.
    ///
    /// Deliberately does NOT expose the amount: a state's `amount` field is
    /// reassigned to the conversion's received amount (and later the post-fee
    /// `amount_received`), so it is not the original requested amount the CLI
    /// prints -- validating `--amount` against it would reject legitimate resumes
    /// whenever slippage or CCTP fees move the effective amount.
    pub(crate) fn direction(&self) -> RebalanceDirection {
        match self {
            Self::Converting { direction, .. }
            | Self::ConversionComplete { direction, .. }
            | Self::ConversionFailed { direction, .. }
            | Self::WithdrawalSubmitting { direction, .. }
            | Self::Withdrawing { direction, .. }
            | Self::WithdrawalComplete { direction, .. }
            | Self::WithdrawalFailed { direction, .. }
            | Self::BridgingSubmitting { direction, .. }
            | Self::Bridging { direction, .. }
            | Self::AwaitingAttestation { direction, .. }
            | Self::Attested { direction, .. }
            | Self::Bridged { direction, .. }
            | Self::BridgingFailed { direction, .. }
            | Self::DepositInitiated { direction, .. }
            | Self::DepositConfirmed { direction, .. }
            | Self::DepositFailed { direction, .. }
            | Self::Reconciled { direction, .. } => *direction,
        }
    }

    /// Returns the data needed to reconstruct an in-memory tracking entry
    /// during startup guard recovery.
    ///
    /// Returns `Some((direction, amount, last_progress_at))` for the three
    /// manually-reconcilable guard-holding terminal states that cannot
    /// self-recover via the reactor/apalis:
    ///
    /// - `DepositFailed` (any direction): post-burn/post-mint; no recovery
    ///   path.
    /// - `ConversionFailed { direction: BaseToAlpaca }`: the post-deposit
    ///   USDC->USD conversion leg; post-mint, no apalis re-arm.
    /// - `BridgingFailed { direction: AlpacaToBase, burn_tx_hash: Some }`:
    ///   post-burn, no recovery job (only `BaseToAlpaca` is re-armed on
    ///   startup via `resumable_post_burn_transfer`).
    ///
    /// All three are accepted by `transition_reconcile_stuck_rebalance` (the
    /// CLI `transfer reconcile` target), hold the guard on restart
    /// (`holds_rebalance_guard` returns `true`), and are NOT auto-re-armed:
    /// without a tracking seed the sweep has no entry to iterate and the guard
    /// stays latched forever after a CLI reconcile.
    ///
    /// All other states return `None`:
    /// - Guard-holding in-progress states self-recover via the reactor/apalis
    ///   once resumed; seeding tracking would wedge their `DepositConfirmed`
    ///   path (which requires `bridged_amount_received`) and corrupt inflight
    ///   accounting.
    /// - `BridgingFailed { direction: BaseToAlpaca, burn_tx_hash: Some }` is
    ///   resumable and re-armed by `recover_usdc_guard` via
    ///   `resumable_post_burn_transfer`; seeding it would wedge the re-armed
    ///   job's `DepositConfirmed` path.
    /// - Non-guard-holding terminal states are skipped entirely by the caller.
    ///
    /// `recover_usdc_guard` still re-latches the guard for all guard-holding
    /// states; only the tracking seed is narrowed here.
    pub(crate) fn guard_recovery_tracking_data(
        &self,
    ) -> Option<(RebalanceDirection, Usdc, DateTime<Utc>)> {
        match self {
            // The three manually-reconcilable guard-holding terminal states
            // that cannot self-recover via the reactor/apalis:
            //  - DepositFailed (any direction): post-burn/post-mint.
            //  - ConversionFailed(BaseToAlpaca): post-deposit USDC->USD leg,
            //    post-mint, no apalis re-arm.
            //  - BridgingFailed(AlpacaToBase, burn_tx=Some): post-burn; only
            //    BaseToAlpaca BridgingFailed is re-armed on startup by
            //    `resumable_post_burn_transfer`; AlpacaToBase has no recovery
            //    path and requires CLI reconciliation.
            Self::DepositFailed {
                direction,
                amount,
                failed_at,
                ..
            }
            | Self::ConversionFailed {
                direction: direction @ RebalanceDirection::BaseToAlpaca,
                amount,
                failed_at,
                ..
            }
            | Self::BridgingFailed {
                direction: direction @ RebalanceDirection::AlpacaToBase,
                amount,
                burn_tx_hash: Some(_),
                failed_at,
                ..
            } => Some((*direction, *amount, *failed_at)),
            // Guard-holding in-progress states: self-recover via
            // reactor/apalis once resumed; seeding would wedge
            // `DepositConfirmed`.
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            // ConversionFailed(AlpacaToBase) is pre-withdrawal (pre-burn),
            // not guard-holding; caller skips it.
            | Self::ConversionFailed { direction: RebalanceDirection::AlpacaToBase, .. }
            // BridgingFailed(BaseToAlpaca, burn_tx=Some): resumable, re-armed
            // by recover_usdc_guard; seeding would wedge its DepositConfirmed
            // path.
            | Self::BridgingFailed {
                direction: RebalanceDirection::BaseToAlpaca,
                burn_tx_hash: Some(_),
                ..
            }
            // BridgingFailed(burn_tx=None): pre-burn, not guard-holding;
            // caller skips it. cctp_nonce-only case also not guard-holding.
            | Self::BridgingFailed { burn_tx_hash: None, .. }
            // Non-guard-holding terminals: no seeding needed.
            | Self::WithdrawalFailed { .. }
            | Self::Reconciled { .. } => None,
        }
    }

    /// The persisted effective amount of the transfer (post-conversion /
    /// post-fee where applicable), as opposed to whatever an operator typed
    /// on a CLI resume.
    pub(crate) fn amount(&self) -> Usdc {
        match self {
            // Post-conversion / post-bridge, the actually received amount is
            // the effective one; `amount` is the originally requested figure.
            Self::ConversionComplete { conversion, .. } => conversion.received_amount,
            Self::Bridged {
                amount_received, ..
            } => *amount_received,
            // The burn adopts the post-withdrawal-fee `burn_amount` when one was
            // persisted -- matching the `BridgingInitiated` transition and the
            // manager's resume scan -- so the effective amount is that, not the
            // nominal. `None` (aggregates persisted before `burn_amount`) falls
            // back to the nominal `amount`.
            Self::BridgingSubmitting {
                amount,
                burn_amount,
                ..
            } => burn_amount.unwrap_or(*amount),
            Self::Converting { amount, .. }
            | Self::ConversionFailed { amount, .. }
            | Self::WithdrawalSubmitting { amount, .. }
            | Self::Withdrawing { amount, .. }
            | Self::WithdrawalComplete { amount, .. }
            | Self::WithdrawalFailed { amount, .. }
            | Self::Bridging { amount, .. }
            | Self::AwaitingAttestation { amount, .. }
            | Self::Attested { amount, .. }
            | Self::BridgingFailed { amount, .. }
            | Self::DepositInitiated { amount, .. }
            | Self::DepositConfirmed { amount, .. }
            | Self::DepositFailed { amount, .. }
            | Self::Reconciled { amount, .. } => *amount,
        }
    }

    /// Returns the `(direction, amount)` for a mid-flight resumable aggregate
    /// that can be re-armed by `recover_usdc_guard`, or `None` otherwise.
    ///
    /// `BridgingSubmitting` is resumable for both directions.
    /// `WithdrawalSubmitting` is resumable only for `BaseToAlpaca`:
    /// `resume_base_to_alpaca` handles it; `resume_alpaca_to_base` returns
    /// `ResumeDirectionMismatch` for `WithdrawalSubmitting{AlpacaToBase}`.
    /// `Withdrawing{AlpacaToBase, AlpacaId}` is resumable: the
    /// `AlpacaTransferId` is durably recorded and `resume_alpaca_to_base`
    /// re-polls the same transfer (idempotent). `WithdrawalComplete` in the
    /// `AlpacaToBase` direction is also resumable: the source withdrawal has
    /// already moved funds and the resume path scans for an existing burn before
    /// submitting one. `Withdrawing{BaseToAlpaca}`,
    /// `Withdrawing{AlpacaToBase, OnchainTx}`, and
    /// `WithdrawalComplete{BaseToAlpaca}` are NOT resumable here: those paths
    /// use on-chain txs, handled by the post-burn path.
    ///
    /// Post-burn states (`Bridging`, `AwaitingAttestation`, `Attested`) are
    /// handled by `resumable_post_burn_transfer`.
    pub(crate) fn is_resumable_mid_flight_data(&self) -> Option<(RebalanceDirection, Usdc)> {
        match self {
            // WithdrawalSubmitting is only resumable for BaseToAlpaca:
            // resume_base_to_alpaca handles it; resume_alpaca_to_base returns
            // ResumeDirectionMismatch for this state regardless of direction.
            Self::WithdrawalSubmitting {
                direction: direction @ RebalanceDirection::BaseToAlpaca,
                amount,
                ..
            }
            // AlpacaToBase `Withdrawing`: the AlpacaTransferId is durably
            // recorded; `resume_alpaca_to_base` re-polls the same transfer
            // (idempotent). `WithdrawalComplete{AlpacaToBase}` is one step
            // further: funds have left Alpaca, and resume scans for any existing
            // burn before submitting one. Both are safe to re-arm on startup.
            // `WithdrawalSubmitting{AlpacaToBase}` remains non-re-armable
            // because the Alpaca transfer ID is not persisted in that state.
            | Self::Withdrawing {
                direction: direction @ RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal_ref: TransferRef::AlpacaId(_),
                ..
            }
            | Self::WithdrawalComplete {
                direction: direction @ RebalanceDirection::AlpacaToBase,
                amount,
                ..
            }
            | Self::BridgingSubmitting {
                direction, amount, ..
            } => Some((*direction, *amount)),
            Self::WithdrawalSubmitting {
                direction: RebalanceDirection::AlpacaToBase,
                ..
            }
            | Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete {
                direction: RebalanceDirection::BaseToAlpaca,
                ..
            }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::BridgingFailed { .. }
            | Self::Reconciled { .. } => None,
        }
    }

    /// Returns the exact source reservation that an Alpaca-to-Base pre-burn
    /// aggregate must regain after restart, or `None` when the aggregate is not
    /// in a recoverable source-reservation state.
    pub(crate) fn alpaca_to_base_reservation_recovery(
        &self,
    ) -> Option<AlpacaToBaseReservationRecovery> {
        let (amount, stage, last_progress_at) = match self {
            Self::ConversionComplete {
                direction: RebalanceDirection::AlpacaToBase,
                conversion,
                converted_at,
                ..
            } => (
                conversion.received_amount,
                AlpacaToBaseReservationStage::ConversionComplete,
                *converted_at,
            ),
            Self::Withdrawing {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                initiated_at,
                ..
            } => (
                *amount,
                AlpacaToBaseReservationStage::Withdrawing,
                *initiated_at,
            ),
            Self::WithdrawalComplete {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                confirmed_at,
                ..
            } => (
                *amount,
                AlpacaToBaseReservationStage::WithdrawalComplete,
                *confirmed_at,
            ),
            Self::BridgingSubmitting {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                initiated_at,
                ..
            } => (
                *amount,
                AlpacaToBaseReservationStage::BridgingSubmitting,
                *initiated_at,
            ),
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. }
            | Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => return None,
        };

        Some(AlpacaToBaseReservationRecovery {
            amount,
            stage,
            last_progress_at,
        })
    }
}

/// Candidate `UsdcRebalance` aggregates whose latest event leaves them
/// potentially holding the single-rebalance guard, for startup recovery, split
/// by whether their persisted `aggregate_id` parsed.
///
/// `unparseable` rows cannot be loaded or classified, so the recovery path must
/// fail closed and hold the guard for them rather than silently dropping them --
/// dropping would leave the guard clear for a possibly-post-burn rebalance and
/// re-open the re-burn window. The raw ids are retained for diagnostics.
pub(crate) struct InterruptedUsdcRebalances {
    pub(crate) ids: Vec<UsdcRebalanceId>,
    pub(crate) unparseable: Vec<String>,
}

/// Returns the candidate `UsdcRebalance` aggregates whose latest event leaves
/// them potentially holding the single-rebalance guard, for startup recovery.
///
/// The `event_type` filter is a coarse pre-filter: it excludes only
/// `WithdrawalFailed` (always pre-burn, reconciles to source) and keeps every
/// other latest event -- including `ConversionFailed` (post-burn for
/// BaseToAlpaca's post-deposit leg, pre-burn for AlpacaToBase) and the events
/// that are terminal in one direction but intermediate in the other
/// (`ConversionConfirmed`, `DepositConfirmed`). The caller loads each aggregate
/// and applies [`UsdcRebalance::holds_rebalance_guard`] for the precise,
/// direction-aware decision. USDC rebalances are infrequent, large operations,
/// so loading the recently-terminal ones to filter them out is cheap.
pub(crate) async fn interrupted_usdc_rebalance_ids(
    pool: &SqlitePool,
) -> Result<InterruptedUsdcRebalances, sqlx::Error> {
    let rows: Vec<String> = sqlx::query_scalar(
        "WITH latest AS ( \
             SELECT aggregate_id, MAX(sequence) AS max_seq \
             FROM events \
             WHERE aggregate_type = 'UsdcRebalance' \
             GROUP BY aggregate_id \
         ) \
         SELECT latest.aggregate_id \
         FROM events last_ev \
         INNER JOIN latest \
             ON last_ev.aggregate_id = latest.aggregate_id \
            AND last_ev.sequence = latest.max_seq \
         WHERE last_ev.aggregate_type = 'UsdcRebalance' \
           AND last_ev.event_type IN ( \
               'UsdcRebalanceEvent::ConversionInitiated', \
               'UsdcRebalanceEvent::ConversionConfirmed', \
               'UsdcRebalanceEvent::ConversionFailed', \
               'UsdcRebalanceEvent::Initiated', \
               'UsdcRebalanceEvent::WithdrawalSubmitting', \
               'UsdcRebalanceEvent::WithdrawalConfirmed', \
               'UsdcRebalanceEvent::BridgingSubmitting', \
               'UsdcRebalanceEvent::PendingBurnRecorded', \
               'UsdcRebalanceEvent::PendingBurnCleared', \
               'UsdcRebalanceEvent::BridgingInitiated', \
               'UsdcRebalanceEvent::BridgeAttestationReceived', \
               'UsdcRebalanceEvent::AttestationTimedOut', \
               'UsdcRebalanceEvent::Bridged', \
               'UsdcRebalanceEvent::BridgingFailed', \
               'UsdcRebalanceEvent::BridgingCompletionRecovered', \
               'UsdcRebalanceEvent::DepositInitiated', \
               'UsdcRebalanceEvent::DepositConfirmed', \
               'UsdcRebalanceEvent::DepositFailed' \
           ) \
         ORDER BY latest.aggregate_id",
    )
    .fetch_all(pool)
    .await?;

    let mut ids = Vec::new();
    let mut unparseable = Vec::new();
    for raw in rows {
        match raw.parse::<UsdcRebalanceId>() {
            Ok(id) => ids.push(id),
            Err(error) => {
                warn!(
                    target: "rebalance",
                    %raw, ?error,
                    "Unparseable UsdcRebalance aggregate_id during guard recovery; \
                     holding guard defensively"
                );
                unparseable.push(raw);
            }
        }
    }

    Ok(InterruptedUsdcRebalances { ids, unparseable })
}

impl EventSourced for UsdcRebalance {
    type Id = UsdcRebalanceId;
    type Event = UsdcRebalanceEvent;
    type Command = UsdcRebalanceCommand;
    type Error = UsdcRebalanceError;
    type Jobs = Nil;
    type Materialized = Nil;

    const AGGREGATE_TYPE: &'static str = "UsdcRebalance";
    const PROJECTION: Nil = Nil;
    // v2: cctp_nonce widened from u64 to B256 (full CCTP V2 nonce) in the
    // Attested/BridgingFailed state variants. Bumped to clear stale snapshots
    // so they rebuild from events. No event upcaster needed: persisted events
    // only ever stored cctp_nonce=null (the nonce bug prevented any successful
    // attestation), which deserializes unchanged into Option<B256>.
    // v3: added the non-terminal AwaitingAttestation state so attestation
    // timeouts replay as retryable instead of terminal bridge failures.
    // v4: added the `message` envelope field to the Attested state and the
    // BridgeAttestationReceived event. The field is `Option<Vec<u8>>`, so serde
    // deserializes both a stale snapshot and a pre-field event with a missing
    // `message` as `None` -- legacy `Attested` transfers stay loadable and
    // resume via the re-poll fallback either way. Bumped (defensively, matching
    // v2/v3) to clear stale snapshots and rebuild from events, so any persisted
    // `Attested` snapshot is regenerated under the current schema rather than
    // relying on serde's missing-Option-as-None leniency.
    // v5: added the terminal `Reconciled` state (and `OperatorReconciled` event)
    // for operator reconciliation of a post-burn `DepositFailed`. Bumped to
    // clear stale snapshots so they rebuild from events under the new schema.
    // v6: added `failure_reason: Option<String>` to the `Reconciled` state.
    // Existing v5 snapshots of a `Reconciled` aggregate would deserialize
    // `failure_reason` as `None` via serde default and the historical-recovery
    // in `evolve()` would never run for them. Bumped to invalidate stale
    // snapshots, forcing a rebuild from events so `failure_reason` is derived
    // from the prior failed state on replay.
    // v7: the conversion data model changed -- `ConversionComplete` state and the
    // `ConversionConfirmed` event now carry `ConversionAmounts` (source +
    // received) instead of a single `filled_amount`. Bumped to clear stale
    // snapshots so they rebuild under the new schema.
    // v8: `Withdrawing.initiated_at` semantics changed -- it now records the
    // withdrawal initiation time (from the `Initiated` event's `initiated_at`
    // field) rather than the overall rebalance start. Stale `Withdrawing`
    // snapshots carry the old timestamp, which would skew the 4-hour operator
    // alert deadline. Bumped so snapshots are cleared and `initiated_at` is
    // rebuilt from events on the next load.
    const SCHEMA_VERSION: u64 = 8;

    fn originate(event: &Self::Event) -> Option<Self> {
        use UsdcRebalanceEvent::*;
        match event {
            ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at,
            } => Some(Self::Converting {
                direction: *direction,
                amount: *amount,
                order_id: order_id.clone(),
                initiated_at: *initiated_at,
            }),

            WithdrawalSubmitting {
                direction,
                amount,
                from_block,
                submitting_at,
            } => Some(Self::WithdrawalSubmitting {
                direction: *direction,
                amount: *amount,
                from_block: *from_block,
                initiated_at: *submitting_at,
            }),

            Initiated {
                direction,
                amount,
                withdrawal_ref,
                initiated_at,
            } => Some(Self::Withdrawing {
                direction: *direction,
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *initiated_at,
            }),

            _ => None,
        }
    }

    // All logic is a single pattern match mapping (event, state) -> next_state.
    // Extracting arms into helpers would only add indirection without
    // improving readability.
    #[expect(clippy::too_many_lines)]
    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use UsdcRebalanceEvent::*;

        let next = match (event, entity) {
            (
                ConversionInitiated {
                    direction,
                    amount,
                    order_id,
                    ..
                },
                Self::DepositConfirmed { initiated_at, .. },
            ) => Self::Converting {
                direction: *direction,
                amount: *amount,
                order_id: order_id.clone(),
                initiated_at: *initiated_at,
            },

            (
                ConversionConfirmed {
                    conversion,
                    converted_at,
                    ..
                },
                Self::Converting {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::ConversionComplete {
                direction: *direction,
                amount: *amount,
                conversion: *conversion,
                initiated_at: *initiated_at,
                converted_at: *converted_at,
            },

            (
                ConversionFailed { reason, failed_at },
                Self::Converting {
                    direction,
                    amount,
                    order_id,
                    initiated_at,
                },
            ) => Self::ConversionFailed {
                direction: *direction,
                amount: *amount,
                order_id: order_id.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            (
                WithdrawalSubmitting { from_block, .. },
                Self::ConversionComplete {
                    direction,
                    conversion,
                    initiated_at,
                    ..
                },
            ) => Self::WithdrawalSubmitting {
                direction: *direction,
                amount: conversion.received_amount,
                from_block: *from_block,
                initiated_at: *initiated_at,
            },

            (
                Initiated {
                    withdrawal_ref,
                    initiated_at: withdrawal_initiated_at,
                    ..
                },
                Self::WithdrawalSubmitting {
                    direction, amount, ..
                },
            ) => Self::Withdrawing {
                direction: *direction,
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                // Use the event's initiated_at (set by transition_initiate_withdrawal to
                // Utc::now() at the moment the Alpaca withdrawal is initiated), NOT the
                // prior state's initiated_at which tracks the overall rebalance start.
                // This is the correct epoch for the durable 4-hour operator alert deadline.
                initiated_at: *withdrawal_initiated_at,
            },

            (
                Initiated {
                    withdrawal_ref,
                    initiated_at: withdrawal_initiated_at,
                    ..
                },
                Self::ConversionComplete {
                    direction,
                    conversion,
                    ..
                },
            ) => Self::Withdrawing {
                direction: *direction,
                amount: conversion.received_amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *withdrawal_initiated_at,
            },

            (
                WithdrawalConfirmed {
                    confirmed_at,
                    withdrawal_tx,
                },
                Self::Withdrawing {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::WithdrawalComplete {
                direction: *direction,
                amount: *amount,
                initiated_at: *initiated_at,
                confirmed_at: *confirmed_at,
                withdrawal_tx: *withdrawal_tx,
            },

            (
                WithdrawalFailed { reason, failed_at },
                Self::Withdrawing {
                    direction,
                    amount,
                    withdrawal_ref,
                    initiated_at,
                },
            ) => Self::WithdrawalFailed {
                direction: *direction,
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            // Two transitions converge on `BridgingSubmitting` with no pending burn
            // tx: opening the bridging phase from `WithdrawalComplete`, and clearing
            // a recorded pending burn (via `ClearPendingBurn`) before a (re)broadcast
            // so a failed re-record fails closed instead of reburning off a stale hash.
            (
                BridgingSubmitting {
                    from_block,
                    burn_amount,
                    ..
                },
                Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            )
            | (
                PendingBurnCleared { .. },
                Self::BridgingSubmitting {
                    direction,
                    amount,
                    from_block,
                    initiated_at,
                    burn_amount,
                    ..
                },
            ) => Self::BridgingSubmitting {
                direction: *direction,
                amount: *amount,
                from_block: *from_block,
                initiated_at: *initiated_at,
                burn_amount: *burn_amount,
                pending_burn_tx: None,
            },

            (
                PendingBurnRecorded { burn_tx, .. },
                Self::BridgingSubmitting {
                    direction,
                    amount,
                    from_block,
                    initiated_at,
                    burn_amount,
                    ..
                },
            ) => Self::BridgingSubmitting {
                direction: *direction,
                amount: *amount,
                from_block: *from_block,
                initiated_at: *initiated_at,
                burn_amount: *burn_amount,
                pending_burn_tx: Some(*burn_tx),
            },

            (
                BridgingInitiated {
                    burn_tx_hash,
                    burned_at,
                },
                Self::BridgingSubmitting {
                    direction,
                    amount,
                    initiated_at,
                    burn_amount,
                    ..
                },
            ) => Self::Bridging {
                direction: *direction,
                // Carry the actual burned amount (what was received after Alpaca
                // withdrawal fees) into post-burn states so DTOs, recovery
                // classification, and reconciliation see what was really burned,
                // not the nominal. `None` for aggregates persisted before
                // `burn_amount` existed, which fall back to the nominal amount.
                amount: burn_amount.unwrap_or(*amount),
                burn_tx_hash: *burn_tx_hash,
                initiated_at: *initiated_at,
                burned_at: *burned_at,
            },

            (
                BridgingInitiated {
                    burn_tx_hash,
                    burned_at,
                },
                Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::Bridging {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                initiated_at: *initiated_at,
                burned_at: *burned_at,
            },

            (
                BridgeAttestationReceived {
                    attestation,
                    cctp_nonce,
                    message,
                    mint_scan_from_block,
                    attested_at,
                },
                Self::Bridging {
                    direction,
                    amount,
                    burn_tx_hash,
                    initiated_at,
                    ..
                }
                | Self::AwaitingAttestation {
                    direction,
                    amount,
                    burn_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::Attested {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                cctp_nonce: *cctp_nonce,
                attestation: attestation.clone(),
                message: message.clone(),
                mint_scan_from_block: *mint_scan_from_block,
                initiated_at: *initiated_at,
                attested_at: *attested_at,
            },

            (
                AttestationTimedOut {
                    burn_tx_hash,
                    retry_deadline_at,
                    timed_out_at,
                },
                Self::Bridging {
                    direction,
                    amount,
                    burn_tx_hash: state_burn_tx_hash,
                    initiated_at,
                    ..
                },
            ) if burn_tx_hash == state_burn_tx_hash => Self::AwaitingAttestation {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                initiated_at: *initiated_at,
                timed_out_at: *timed_out_at,
                retry_deadline_at: *retry_deadline_at,
            },

            (
                Bridged {
                    mint_tx_hash,
                    amount_received,
                    fee_collected,
                    minted_at,
                },
                Self::Attested {
                    direction,
                    amount,
                    burn_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::Bridged {
                direction: *direction,
                amount: *amount,
                amount_received: *amount_received,
                fee_collected: *fee_collected,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                initiated_at: *initiated_at,
                minted_at: *minted_at,
            },

            // Un-fail a post-burn `BridgingFailed` whose mint landed on-chain.
            // Only matches when the failed state carries a burn tx (post-burn);
            // a pre-burn `BridgingFailed` (burn_tx_hash None) has no mint to
            // adopt and falls through to an invalid transition.
            (
                BridgingCompletionRecovered {
                    mint_tx_hash,
                    amount_received,
                    fee_collected,
                    recovered_at,
                },
                Self::BridgingFailed {
                    direction,
                    amount,
                    burn_tx_hash: Some(burn_tx_hash),
                    initiated_at,
                    ..
                },
            ) => Self::Bridged {
                direction: *direction,
                amount: *amount,
                amount_received: *amount_received,
                fee_collected: *fee_collected,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                initiated_at: *initiated_at,
                minted_at: *recovered_at,
            },

            (
                BridgingFailed {
                    burn_tx_hash,
                    cctp_nonce,
                    reason,
                    failed_at,
                },
                Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                }
                | Self::BridgingSubmitting {
                    direction,
                    amount,
                    initiated_at,
                    ..
                }
                | Self::Bridging {
                    direction,
                    amount,
                    initiated_at,
                    ..
                }
                | Self::AwaitingAttestation {
                    direction,
                    amount,
                    initiated_at,
                    ..
                }
                | Self::Attested {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::BridgingFailed {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                cctp_nonce: *cctp_nonce,
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            (
                DepositInitiated {
                    deposit_ref,
                    deposit_initiated_at,
                },
                Self::Bridged {
                    direction,
                    amount_received,
                    burn_tx_hash,
                    mint_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::DepositInitiated {
                direction: *direction,
                amount: *amount_received,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                deposit_ref: deposit_ref.clone(),
                initiated_at: *initiated_at,
                deposit_initiated_at: *deposit_initiated_at,
            },

            (
                DepositConfirmed {
                    deposit_confirmed_at,
                    ..
                },
                Self::DepositInitiated {
                    direction,
                    amount,
                    burn_tx_hash,
                    mint_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::DepositConfirmed {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                initiated_at: *initiated_at,
                deposit_confirmed_at: *deposit_confirmed_at,
            },

            (
                DepositFailed {
                    deposit_ref,
                    reason,
                    failed_at,
                },
                Self::DepositInitiated {
                    direction,
                    amount,
                    burn_tx_hash,
                    mint_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::DepositFailed {
                direction: *direction,
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                deposit_ref: deposit_ref.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            // OperatorReconciled: derive failure_reason from the prior state and
            // transition to Reconciled, or return Ok(None) for non-reconcilable
            // states. The nested match enumerates every UsdcRebalance variant
            // exhaustively (no wildcard), so adding a new variant forces a
            // conscious classification here at compile time.
            //
            // Deriving failure_reason on replay also covers historical
            // OperatorReconciled events persisted before failure_reason was
            // tracked: the correct reason is recovered regardless of event age.
            (
                OperatorReconciled {
                    direction,
                    amount,
                    reason,
                    initiated_at,
                    reconciled_at,
                },
                state,
            ) => {
                match state {
                    // DepositFailed and ConversionFailed(BaseToAlpaca): post-burn/post-mint,
                    // manually reconcilable.
                    Self::DepositFailed {
                        reason: failure_reason,
                        ..
                    }
                    | Self::ConversionFailed {
                        reason: failure_reason,
                        direction: RebalanceDirection::BaseToAlpaca,
                        ..
                    } => Self::Reconciled {
                        direction: *direction,
                        amount: *amount,
                        reason: *reason,
                        failure_reason: Some(failure_reason.clone()),
                        initiated_at: *initiated_at,
                        reconciled_at: *reconciled_at,
                    },

                    // BridgingFailed with a burn tx or cctp nonce: post-burn, manually
                    // reconcilable.
                    Self::BridgingFailed {
                        reason: failure_reason,
                        burn_tx_hash,
                        cctp_nonce,
                        ..
                    } if burn_tx_hash.is_some() || cctp_nonce.is_some() => Self::Reconciled {
                        direction: *direction,
                        amount: *amount,
                        reason: *reason,
                        failure_reason: Some(failure_reason.clone()),
                        initiated_at: *initiated_at,
                        reconciled_at: *reconciled_at,
                    },

                    // All remaining states are not reconcilable. Enumerated without a
                    // wildcard so the compiler rejects unclassified new variants.
                    Self::Converting { .. }
                    | Self::ConversionComplete { .. }
                    | Self::ConversionFailed {
                        direction: RebalanceDirection::AlpacaToBase,
                        ..
                    }
                    | Self::WithdrawalSubmitting { .. }
                    | Self::Withdrawing { .. }
                    | Self::WithdrawalComplete { .. }
                    | Self::WithdrawalFailed { .. }
                    | Self::BridgingSubmitting { .. }
                    | Self::Bridging { .. }
                    | Self::AwaitingAttestation { .. }
                    | Self::Attested { .. }
                    | Self::BridgingFailed { .. }
                    | Self::Bridged { .. }
                    | Self::DepositInitiated { .. }
                    | Self::DepositConfirmed { .. }
                    | Self::Reconciled { .. } => return Ok(None),
                }
            }

            _ => return Ok(None),
        };

        Ok(Some(next))
    }

    fn initialize(
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UsdcRebalanceCommand::*;
        use UsdcRebalanceEvent::*;

        match command {
            InitiateConversion {
                direction,
                amount,
                order_id,
            } => Ok(vec![ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at: Utc::now(),
            }]),

            #[cfg(any(test, feature = "test-support"))]
            InitiateConversionAt {
                direction,
                amount,
                order_id,
                initiated_at,
            } => Ok(vec![ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at,
            }]),

            BeginWithdrawal {
                direction,
                amount,
                from_block,
            } => Self::begin_withdrawal_init_events(direction, amount, from_block, Utc::now()),

            #[cfg(any(test, feature = "test-support"))]
            BeginWithdrawalAt {
                direction,
                amount,
                from_block,
                submitting_at,
            } => Self::begin_withdrawal_init_events(direction, amount, from_block, submitting_at),

            Initiate {
                direction,
                amount,
                withdrawal,
            } => Ok(vec![Initiated {
                direction,
                amount,
                withdrawal_ref: withdrawal,
                initiated_at: Utc::now(),
            }]),

            #[cfg(any(test, feature = "test-support"))]
            InitiateAt {
                direction,
                amount,
                withdrawal,
                initiated_at,
            } => Ok(vec![Initiated {
                direction,
                amount,
                withdrawal_ref: withdrawal,
                initiated_at,
            }]),

            ConfirmConversion { .. } | FailConversion { .. } => {
                Err(UsdcRebalanceError::ConversionNotInitiated)
            }
            #[cfg(any(test, feature = "test-support"))]
            ConfirmConversionAt { .. } => Err(UsdcRebalanceError::ConversionNotInitiated),

            InitiatePostDepositConversion { .. } => Err(UsdcRebalanceError::DepositNotConfirmed),
            #[cfg(any(test, feature = "test-support"))]
            InitiatePostDepositConversionAt { .. } => Err(UsdcRebalanceError::DepositNotConfirmed),

            ConfirmWithdrawal { .. } | FailWithdrawal { .. } => {
                Err(UsdcRebalanceError::WithdrawalNotInitiated)
            }
            #[cfg(any(test, feature = "test-support"))]
            ConfirmWithdrawalAt { .. } => Err(UsdcRebalanceError::WithdrawalNotInitiated),

            BeginBridging { .. }
            | InitiateBridging { .. }
            | RecordPendingBurn { .. }
            | ClearPendingBurn => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            #[cfg(any(test, feature = "test-support"))]
            BeginBridgingAt { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            #[cfg(any(test, feature = "test-support"))]
            InitiateBridgingAt { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),

            ReceiveAttestation { .. }
            | TimeoutAttestation { .. }
            | FailBridging { .. }
            | RecoverBridging { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            #[cfg(any(test, feature = "test-support"))]
            ReceiveAttestationAt { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),

            ConfirmBridging { .. } => Err(UsdcRebalanceError::AttestationNotReceived),
            #[cfg(any(test, feature = "test-support"))]
            ConfirmBridgingAt { .. } => Err(UsdcRebalanceError::AttestationNotReceived),

            InitiateDeposit { .. } => Err(UsdcRebalanceError::BridgingNotCompleted),
            #[cfg(any(test, feature = "test-support"))]
            InitiateDepositAt { .. } => Err(UsdcRebalanceError::BridgingNotCompleted),

            ConfirmDeposit | FailDeposit { .. } => Err(UsdcRebalanceError::DepositNotInitiated),
            #[cfg(any(test, feature = "test-support"))]
            ConfirmDepositAt { .. } => Err(UsdcRebalanceError::DepositNotInitiated),

            ReconcileStuckRebalance { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "ReconcileStuckRebalance".to_string(),
                state: "Uninitialized".to_string(),
            }),
        }
    }

    fn transition(
        &self,
        command: Self::Command,
        _jobs: &mut JobQueue<Self::Jobs>,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UsdcRebalanceCommand::*;
        match command {
            InitiateConversion { .. } => Err(UsdcRebalanceError::AlreadyInitiated),
            #[cfg(any(test, feature = "test-support"))]
            InitiateConversionAt { .. } => Err(UsdcRebalanceError::AlreadyInitiated),

            ConfirmConversion { conversion } => {
                self.transition_confirm_conversion(conversion, Utc::now())
            }
            #[cfg(any(test, feature = "test-support"))]
            ConfirmConversionAt {
                conversion,
                converted_at,
            } => self.transition_confirm_conversion(conversion, converted_at),

            FailConversion { reason } => self.transition_fail_conversion(reason),

            InitiatePostDepositConversion { order_id, amount } => {
                self.transition_post_deposit_conversion(order_id, amount, Utc::now())
            }
            #[cfg(any(test, feature = "test-support"))]
            InitiatePostDepositConversionAt {
                order_id,
                amount,
                initiated_at,
            } => self.transition_post_deposit_conversion(order_id, amount, initiated_at),

            BeginWithdrawal {
                direction,
                amount,
                from_block,
            } => self.transition_begin_withdrawal(direction, amount, from_block, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            BeginWithdrawalAt {
                direction,
                amount,
                from_block,
                submitting_at,
            } => self.transition_begin_withdrawal(direction, amount, from_block, submitting_at),

            Initiate {
                direction,
                amount,
                withdrawal,
            } => self.transition_initiate_withdrawal(direction, amount, withdrawal, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            InitiateAt {
                direction,
                amount,
                withdrawal,
                initiated_at,
            } => self.transition_initiate_withdrawal(direction, amount, withdrawal, initiated_at),

            ConfirmWithdrawal { withdrawal_tx } => {
                self.transition_confirm_withdrawal(withdrawal_tx, Utc::now())
            }
            #[cfg(any(test, feature = "test-support"))]
            ConfirmWithdrawalAt {
                withdrawal_tx,
                confirmed_at,
            } => self.transition_confirm_withdrawal(withdrawal_tx, confirmed_at),

            FailWithdrawal { reason } => self.transition_fail_withdrawal(reason),

            BeginBridging {
                from_block,
                burn_amount,
            } => self.transition_begin_bridging(from_block, burn_amount, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            BeginBridgingAt {
                from_block,
                burn_amount,
                submitting_at,
            } => self.transition_begin_bridging(from_block, burn_amount, submitting_at),

            InitiateBridging { burn_tx } => self.transition_initiate_bridging(burn_tx, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            InitiateBridgingAt { burn_tx, burned_at } => {
                self.transition_initiate_bridging(burn_tx, burned_at)
            }

            RecordPendingBurn { burn_tx } => self.transition_record_pending_burn(burn_tx),
            ClearPendingBurn => self.transition_clear_pending_burn(),

            ReceiveAttestation {
                attestation,
                cctp_nonce,
                message,
                mint_scan_from_block,
            } => self.transition_receive_attestation(
                attestation,
                cctp_nonce,
                message,
                mint_scan_from_block,
                Utc::now(),
            ),
            #[cfg(any(test, feature = "test-support"))]
            ReceiveAttestationAt {
                attestation,
                cctp_nonce,
                message,
                mint_scan_from_block,
                attested_at,
            } => self.transition_receive_attestation(
                attestation,
                cctp_nonce,
                message,
                mint_scan_from_block,
                attested_at,
            ),

            TimeoutAttestation { retry_deadline_at } => {
                self.transition_timeout_attestation(retry_deadline_at)
            }

            ConfirmBridging {
                mint_tx,
                amount_received,
                fee_collected,
            } => self.transition_confirm_bridging(
                mint_tx,
                amount_received,
                fee_collected,
                Utc::now(),
            ),
            #[cfg(any(test, feature = "test-support"))]
            ConfirmBridgingAt {
                mint_tx,
                amount_received,
                fee_collected,
                minted_at,
            } => {
                self.transition_confirm_bridging(mint_tx, amount_received, fee_collected, minted_at)
            }

            FailBridging { reason } => self.transition_fail_bridging(reason),
            RecoverBridging {
                mint_tx,
                amount_received,
                fee_collected,
            } => self.transition_recover_bridging(mint_tx, amount_received, fee_collected),

            InitiateDeposit { deposit } => self.transition_initiate_deposit(deposit, Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            InitiateDepositAt {
                deposit,
                deposit_initiated_at,
            } => self.transition_initiate_deposit(deposit, deposit_initiated_at),

            ConfirmDeposit => self.transition_confirm_deposit(Utc::now()),
            #[cfg(any(test, feature = "test-support"))]
            ConfirmDepositAt {
                deposit_confirmed_at,
            } => self.transition_confirm_deposit(deposit_confirmed_at),

            FailDeposit { reason } => self.transition_fail_deposit(reason),
            ReconcileStuckRebalance { reason } => self.transition_reconcile_stuck_rebalance(reason),
        }
    }
}

/// Transition handlers for each command, grouped by rebalance phase.
///
/// Extracted from the `transition` match to satisfy the
/// `too_many_lines` lint while keeping each handler self-contained.
impl UsdcRebalance {
    fn transition_confirm_conversion(
        &self,
        conversion: ConversionAmounts,
        converted_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { direction, .. } => Ok(vec![ConversionConfirmed {
                direction: *direction,
                conversion,
                converted_at,
            }]),
            Self::ConversionComplete { .. } | Self::ConversionFailed { .. } => {
                Err(UsdcRebalanceError::ConversionAlreadyCompleted)
            }
            _ => Err(UsdcRebalanceError::ConversionNotInitiated),
        }
    }

    fn transition_fail_conversion(
        &self,
        reason: String,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. } => Ok(vec![ConversionFailed {
                reason,
                failed_at: Utc::now(),
            }]),
            Self::ConversionComplete { .. } | Self::ConversionFailed { .. } => {
                Err(UsdcRebalanceError::ConversionAlreadyCompleted)
            }
            _ => Err(UsdcRebalanceError::ConversionNotInitiated),
        }
    }

    fn transition_post_deposit_conversion(
        &self,
        order_id: ClientOrderId,
        command_amount: Usdc,
        initiated_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let Self::DepositConfirmed {
            direction, amount, ..
        } = self
        else {
            return Err(UsdcRebalanceError::DepositNotConfirmed);
        };
        if *direction != RebalanceDirection::BaseToAlpaca {
            return Err(UsdcRebalanceError::WrongDirectionForPostDepositConversion);
        }
        if command_amount != *amount {
            return Err(UsdcRebalanceError::ConversionAmountMismatch {
                expected: *amount,
                provided: command_amount,
            });
        }
        Ok(vec![ConversionInitiated {
            direction: *direction,
            amount: *amount,
            order_id,
            initiated_at,
        }])
    }

    /// Records the BaseToAlpaca fresh-start withdrawal intent (the chain
    /// head) before the on-chain withdraw. Shared by `initialize()`'s real
    /// `BeginWithdrawal` command and its fixture-only `BeginWithdrawalAt`
    /// sibling, which differ only in the timestamp source.
    fn begin_withdrawal_init_events(
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        submitting_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        // Fresh-start withdrawal intent is BaseToAlpaca-only. An
        // AlpacaToBase transfer converts USD->USDC first and reaches
        // `WithdrawalSubmitting` through the post-conversion path
        // (`transition_begin_withdrawal` from `ConversionComplete`);
        // recording a fresh AlpacaToBase intent here would materialize a
        // withdrawal with no conversion history.
        if direction != RebalanceDirection::BaseToAlpaca {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "BeginWithdrawal".to_string(),
                state: "Uninitialized (fresh withdrawal is BaseToAlpaca-only; \
                        AlpacaToBase must convert before withdrawing)"
                    .to_string(),
            });
        }
        Ok(vec![UsdcRebalanceEvent::WithdrawalSubmitting {
            direction,
            amount,
            from_block,
            submitting_at,
        }])
    }

    /// Records withdrawal intent (the chain head) before the on-chain
    /// withdraw, for the AlpacaToBase post-conversion path. The BaseToAlpaca
    /// fresh-start path records the same intent via [`Self::initialize`].
    fn transition_begin_withdrawal(
        &self,
        direction: RebalanceDirection,
        amount: Usdc,
        from_block: u64,
        submitting_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let Self::ConversionComplete {
            direction: conv_direction,
            conversion,
            ..
        } = self
        else {
            return Err(UsdcRebalanceError::AlreadyInitiated);
        };
        if direction != *conv_direction {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "BeginWithdrawal".to_string(),
                state: "ConversionComplete with different \
                        direction"
                    .to_string(),
            });
        }
        if amount != conversion.received_amount {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "BeginWithdrawal".to_string(),
                state: format!(
                    "ConversionComplete with amount \
                     mismatch: expected {:?}, got {:?}",
                    conversion.received_amount.inner(),
                    amount.inner()
                ),
            });
        }
        Ok(vec![WithdrawalSubmitting {
            direction,
            amount: conversion.received_amount,
            from_block,
            submitting_at,
        }])
    }

    /// Records the submitted withdrawal transaction, advancing to `Withdrawing`.
    /// Valid from `WithdrawalSubmitting` (the crash-safe path the manager uses)
    /// or directly from `ConversionComplete` (AlpacaToBase post-conversion).
    fn transition_initiate_withdrawal(
        &self,
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
        initiated_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let (expected_direction, expected_amount) = match self {
            Self::WithdrawalSubmitting {
                direction, amount, ..
            } => (direction, amount),
            Self::ConversionComplete {
                direction,
                conversion,
                ..
            } => (direction, &conversion.received_amount),
            _ => return Err(UsdcRebalanceError::AlreadyInitiated),
        };
        if direction != *expected_direction {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "Initiate".to_string(),
                state: "withdrawal entry state with different \
                        direction"
                    .to_string(),
            });
        }
        if amount != *expected_amount {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "Initiate".to_string(),
                state: format!(
                    "withdrawal entry state with amount \
                     mismatch: expected {:?}, got {:?}",
                    expected_amount.inner(),
                    amount.inner()
                ),
            });
        }
        Ok(vec![Initiated {
            direction,
            amount: *expected_amount,
            withdrawal_ref: withdrawal,
            initiated_at,
        }])
    }

    fn transition_confirm_withdrawal(
        &self,
        withdrawal_tx: Option<TxHash>,
        confirmed_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Withdrawing { .. } => Ok(vec![WithdrawalConfirmed {
                confirmed_at,
                withdrawal_tx,
            }]),
            Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            _ => Err(UsdcRebalanceError::WithdrawalNotInitiated),
        }
    }

    fn transition_fail_withdrawal(
        &self,
        reason: String,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Withdrawing { .. } => Ok(vec![WithdrawalFailed {
                reason,
                failed_at: Utc::now(),
            }]),
            Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            _ => Err(UsdcRebalanceError::WithdrawalNotInitiated),
        }
    }

    /// Records bridging intent (the chain head) before the on-chain CCTP burn.
    /// Valid only from `WithdrawalComplete`.
    fn transition_begin_bridging(
        &self,
        from_block: u64,
        burn_amount: Option<Usdc>,
        submitting_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            Self::WithdrawalComplete { amount, .. } => {
                // Reject a burn amount outside `(0, nominal]` before persisting:
                // zero or above-nominal both signal a broken upstream invariant
                // that crash-safe resume must not trust. `None` is the legacy
                // path and stays accepted.
                //
                // Use the fallible `Float` comparisons and FAIL CLOSED: `Usdc`'s
                // `PartialEq`/`PartialOrd` collapse a compare error to `false`,
                // so a plain `==`/`>` would let an un-comparable burn amount slip
                // through into durable `BridgingSubmitting`. A compare error is
                // logged and surfaced as `BurnAmountUncomparable` instead.
                if let Some(burn_amount) = burn_amount {
                    let out_of_range = burn_amount
                        .eq(&Usdc::ZERO)
                        .and_then(|is_zero| Ok(is_zero || burn_amount.gt(amount)?))
                        .inspect_err(|error| {
                            warn!(
                                ?error,
                                %burn_amount,
                                nominal = %amount,
                                "Failed to compare burn amount against valid range; failing closed",
                            );
                        })
                        .map_err(|_| UsdcRebalanceError::BurnAmountUncomparable {
                            burn_amount,
                            nominal: *amount,
                        })?;

                    if out_of_range {
                        return Err(UsdcRebalanceError::InvalidBurnAmount {
                            burn_amount,
                            nominal: *amount,
                        });
                    }
                }

                Ok(vec![BridgingSubmitting {
                    from_block,
                    submitting_at,
                    burn_amount,
                }])
            }
            Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "BeginBridging".to_string(),
                state: "bridging already started".to_string(),
            }),
        }
    }

    /// Records the submitted CCTP burn transaction, advancing the intent state
    /// to `Bridging`. Valid only from `BridgingSubmitting`.
    fn transition_initiate_bridging(
        &self,
        burn_tx: TxHash,
        burned_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            // Valid from `BridgingSubmitting` (the crash-safe path the manager
            // uses) or directly from `WithdrawalComplete`.
            Self::WithdrawalComplete { .. } | Self::BridgingSubmitting { .. } => {
                Ok(vec![BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at,
                }])
            }
            Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateBridging".to_string(),
                state: "Bridging".to_string(),
            }),
        }
    }

    /// Records the broadcast CCTP burn tx hash while still in `BridgingSubmitting`,
    /// before its receipt is awaited, so a crash/timeout redrive checks that exact
    /// tx instead of a mempool-blind log scan. Valid only from `BridgingSubmitting`.
    fn transition_record_pending_burn(
        &self,
        burn_tx: TxHash,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            Self::BridgingSubmitting { .. } => Ok(vec![PendingBurnRecorded {
                burn_tx,
                recorded_at: Utc::now(),
            }]),
            Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyStarted),
        }
    }

    /// Clears any durably-recorded pending burn tx, returning `BridgingSubmitting` to
    /// `pending_burn_tx: None` before a (re)broadcast. A failed re-record then leaves
    /// `pending_burn_tx: None`, so the resume path fails closed instead of reburning off
    /// a stale (reverted) hash. A no-op (no event) when already clear. Valid only from
    /// `BridgingSubmitting`.
    fn transition_clear_pending_burn(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            Self::BridgingSubmitting {
                pending_burn_tx: Some(_),
                ..
            } => Ok(vec![PendingBurnCleared {
                cleared_at: Utc::now(),
            }]),
            Self::BridgingSubmitting {
                pending_burn_tx: None,
                ..
            } => Ok(vec![]),
            Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyStarted),
        }
    }

    fn transition_receive_attestation(
        &self,
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Vec<u8>,
        mint_scan_from_block: u64,
        attested_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            Self::Bridging { .. } | Self::AwaitingAttestation { .. } => {
                Ok(vec![BridgeAttestationReceived {
                    attestation,
                    cctp_nonce,
                    message: Some(message),
                    mint_scan_from_block: Some(mint_scan_from_block),
                    attested_at,
                }])
            }
            Self::Attested { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "ReceiveAttestation".to_string(),
                state: "Attested".to_string(),
            }),
            Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    fn transition_timeout_attestation(
        &self,
        retry_deadline_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            Self::Bridging { burn_tx_hash, .. } => Ok(vec![AttestationTimedOut {
                burn_tx_hash: *burn_tx_hash,
                retry_deadline_at,
                timed_out_at: Utc::now(),
            }]),
            Self::AwaitingAttestation { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "TimeoutAttestation".to_string(),
                state: "AwaitingAttestation".to_string(),
            }),
            Self::Attested { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "TimeoutAttestation".to_string(),
                state: "Attested".to_string(),
            }),
            Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    fn transition_confirm_bridging(
        &self,
        mint_tx: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
        minted_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. } => Err(UsdcRebalanceError::AttestationNotReceived),
            Self::Attested { .. } => Ok(vec![Bridged {
                mint_tx_hash: mint_tx,
                amount_received,
                fee_collected,
                minted_at,
            }]),
            Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    fn transition_fail_bridging(
        &self,
        reason: String,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            // A burn tx hash was durably recorded (`pending_burn_tx: Some`), so a
            // CCTP burn may already be on-chain. Failing as pre-burn would emit
            // `BridgingFailed { burn_tx_hash: None }`, clearing the guard and
            // stranding the burned funds. Reject -- the operator must resume (to
            // adopt/await the recorded burn) or verify on-chain.
            Self::BridgingSubmitting {
                pending_burn_tx: Some(_),
                ..
            } => Err(UsdcRebalanceError::BurnAlreadyRecorded),
            // Pre-burn failure: withdrawal succeeded and (for BridgingSubmitting
            // with no recorded burn) the burn intent was recorded but no burn tx
            // exists yet -- e.g. a USDC-to-U256 conversion error or a burn that
            // failed before any `RecordPendingBurn`/`BridgingInitiated` event.
            Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting {
                pending_burn_tx: None,
                ..
            } => Ok(vec![BridgingFailed {
                burn_tx_hash: None,
                cctp_nonce: None,
                reason,
                failed_at: Utc::now(),
            }]),
            Self::Bridging { burn_tx_hash, .. }
            | Self::AwaitingAttestation { burn_tx_hash, .. } => Ok(vec![BridgingFailed {
                burn_tx_hash: Some(*burn_tx_hash),
                cctp_nonce: None,
                reason,
                failed_at: Utc::now(),
            }]),
            Self::Attested {
                burn_tx_hash,
                cctp_nonce,
                ..
            } => Ok(vec![BridgingFailed {
                burn_tx_hash: Some(*burn_tx_hash),
                cctp_nonce: Some(*cctp_nonce),
                reason,
                failed_at: Utc::now(),
            }]),
            Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    /// Un-fail a post-burn `BridgingFailed` whose CCTP mint is confirmed
    /// on-chain, transitioning back to `Bridged` so the deposit leg completes.
    /// Valid only from a `BridgingFailed` carrying a burn tx: a pre-burn failure
    /// has no mint to adopt and must be re-initiated rather than recovered.
    fn transition_recover_bridging(
        &self,
        mint_tx: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::BridgingFailed {
                burn_tx_hash: Some(_),
                ..
            } => Ok(vec![BridgingCompletionRecovered {
                mint_tx_hash: mint_tx,
                amount_received,
                fee_collected,
                recovered_at: Utc::now(),
            }]),
            Self::BridgingFailed {
                burn_tx_hash: None, ..
            } => Err(UsdcRebalanceError::InvalidCommand {
                command: "RecoverBridging".to_string(),
                state: "BridgingFailed (pre-burn: no mint to recover)".to_string(),
            }),
            // Recovery only makes sense for a terminally-failed post-burn
            // bridge; from any other state there is nothing to un-fail.
            _ => Err(UsdcRebalanceError::InvalidCommand {
                command: "RecoverBridging".to_string(),
                state: "non-failed (RecoverBridging is only valid from a \
                        post-burn BridgingFailed)"
                    .to_string(),
            }),
        }
    }

    fn transition_initiate_deposit(
        &self,
        deposit: TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::BridgingFailed { .. } => Err(UsdcRebalanceError::BridgingNotCompleted),
            Self::Bridged { .. } => Ok(vec![DepositInitiated {
                deposit_ref: deposit,
                deposit_initiated_at,
            }]),
            Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateDeposit".to_string(),
                state: format!("{self:?}"),
            }),
        }
    }

    fn transition_confirm_deposit(
        &self,
        deposit_confirmed_at: DateTime<Utc>,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::BridgingFailed { .. }
            | Self::Bridged { .. } => Err(UsdcRebalanceError::DepositNotInitiated),
            Self::DepositInitiated { direction, .. } => Ok(vec![DepositConfirmed {
                direction: *direction,
                deposit_confirmed_at,
            }]),
            Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "ConfirmDeposit".to_string(),
                state: format!("{self:?}"),
            }),
        }
    }

    fn transition_fail_deposit(
        &self,
        reason: String,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::BridgingSubmitting { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::BridgingFailed { .. }
            | Self::Bridged { .. } => Err(UsdcRebalanceError::DepositNotInitiated),
            Self::DepositInitiated { deposit_ref, .. } => Ok(vec![DepositFailed {
                deposit_ref: Some(deposit_ref.clone()),
                reason,
                failed_at: Utc::now(),
            }]),
            Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. }
            | Self::Reconciled { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "FailDeposit".to_string(),
                state: format!("{self:?}"),
            }),
        }
    }

    /// Reconciles a stuck post-burn rebalance to the guard-clearing terminal
    /// `Reconciled` state. The CCTP burn/mint already moved the funds off the
    /// source venue, so the operator settles them out-of-band and this clears
    /// the `usdc_in_progress` guard rather than re-driving the failed leg.
    ///
    /// Valid only from a post-burn terminal failure that strands the guard with
    /// no other exit:
    ///
    /// - `DepositFailed` -- only reachable from `DepositInitiated`, hence always
    ///   post-mint.
    /// - `BridgingFailed` with a recorded `burn_tx_hash` or `cctp_nonce` -- both
    ///   are only ever populated after the burn reaches CCTP, so either proves
    ///   the failure is post-burn (mirrors the reactor's post-burn classifier).
    ///   A pre-burn `BridgingFailed` (both `None`) carries no burned funds and
    ///   already reconciles to source on its own.
    /// - A `BaseToAlpaca` `ConversionFailed` -- that conversion is the
    ///   post-deposit USDC->USD leg, so it is post-mint. The `AlpacaToBase`
    ///   `ConversionFailed` is the pre-withdrawal leg (pre-burn) and reconciles
    ///   to source on its own.
    ///
    /// Every other state is rejected: an in-progress transfer is still being
    /// driven (resume it instead), a pre-burn failure already reconciles to
    /// source, and a terminal success or `Reconciled` has nothing to reconcile.
    fn transition_reconcile_stuck_rebalance(
        &self,
        reason: ReconcileReason,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let (direction, amount, initiated_at) = match self {
            Self::DepositFailed {
                direction,
                amount,
                initiated_at,
                ..
            }
            | Self::ConversionFailed {
                direction: direction @ RebalanceDirection::BaseToAlpaca,
                amount,
                initiated_at,
                ..
            } => (*direction, *amount, *initiated_at),
            Self::BridgingFailed {
                direction,
                amount,
                burn_tx_hash,
                cctp_nonce,
                initiated_at,
                ..
            } if burn_tx_hash.is_some() || cctp_nonce.is_some() => {
                (*direction, *amount, *initiated_at)
            }
            _ => {
                return Err(UsdcRebalanceError::InvalidCommand {
                    command: "ReconcileStuckRebalance".to_string(),
                    state: format!("{self:?}"),
                });
            }
        };

        Ok(vec![OperatorReconciled {
            direction,
            amount,
            reason,
            initiated_at,
            reconciled_at: Utc::now(),
        }])
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use serde_json::{from_value, json, to_value};
    use std::collections::HashSet;
    use uuid::Uuid;

    use st0x_event_sorcery::{LifecycleError, Store, TestHarness, replay, test_store};
    use st0x_float_macro::float;

    use super::*;

    /// Realistic CCTP V2 nonce with non-zero upper bytes -- the actual nonce
    /// from a prod burn that the old u64 validation rejected. Regression guard
    /// against forcing a 32-byte nonce into a u64.
    const TEST_CCTP_NONCE: B256 =
        fixed_bytes!("0xa01ca42d9082e926a81dc287d973a8f072dfa1b20a4fbf7b20f3abda1b376278");
    const OTHER_CCTP_NONCE: B256 =
        fixed_bytes!("0x524cd90eb8dffcb29fcc163aa8258d84e6cc25abc0b4700d704936812ee39824");

    // An event persisted before `mint_scan_from_block` existed must still
    // deserialize on replay -- to `None`, not a hard error -- so older aggregates
    // can be rebuilt after this field was added (the `#[serde(default)]`).
    #[test]
    fn bridge_attestation_received_without_mint_scan_from_block_deserializes_to_none() {
        let old_event = json!({
            "BridgeAttestationReceived": {
                "attestation": [1, 2, 3],
                "cctp_nonce": "0xa01ca42d9082e926a81dc287d973a8f072dfa1b20a4fbf7b20f3abda1b376278",
                "attested_at": "2026-01-01T00:00:00Z"
            }
        });

        let event: UsdcRebalanceEvent =
            from_value(old_event).expect("pre-field event must still deserialize");

        let UsdcRebalanceEvent::BridgeAttestationReceived {
            mint_scan_from_block,
            ..
        } = event
        else {
            panic!("expected BridgeAttestationReceived");
        };

        assert_eq!(mint_scan_from_block, None);
    }

    // An event persisted before the `message` envelope field existed must still
    // deserialize on replay -- to `None`, not a hard error -- so an `Attested`
    // resume of an older aggregate falls back to re-polling Circle rather than
    // failing to rebuild (the `#[serde(default)]`).
    #[test]
    fn bridge_attestation_received_without_message_deserializes_to_none() {
        let old_event = json!({
            "BridgeAttestationReceived": {
                "attestation": [1, 2, 3],
                "cctp_nonce": "0xa01ca42d9082e926a81dc287d973a8f072dfa1b20a4fbf7b20f3abda1b376278",
                "mint_scan_from_block": 100,
                "attested_at": "2026-01-01T00:00:00Z"
            }
        });

        let event: UsdcRebalanceEvent =
            from_value(old_event).expect("pre-field event must still deserialize");

        let UsdcRebalanceEvent::BridgeAttestationReceived { message, .. } = event else {
            panic!("expected BridgeAttestationReceived");
        };

        assert_eq!(message, None);
    }

    // The `message` envelope must survive replay into the `Attested` state so an
    // `Attested` resume can reconstruct the attestation without re-polling Circle.
    #[test]
    fn bridge_attestation_received_carries_message_into_attested() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let envelope = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: Some(envelope.clone()),
                mint_scan_from_block: Some(100),
                attested_at: Utc::now(),
            },
        ])
        .expect("event stream should replay into Attested");

        let Some(UsdcRebalance::Attested { message, .. }) = state else {
            panic!("expected Attested, got {state:?}");
        };

        assert_eq!(message, Some(envelope));
    }

    // A snapshot persisted before the `message` field existed must still load
    // under the current schema: `message` is `Option<Vec<u8>>`, and serde
    // deserializes a missing key for an `Option` field as `None` (no
    // `#[serde(default)]` required). So a stale `Attested` snapshot remains
    // loadable and resumes via the legacy re-poll fallback -- the state-level
    // mirror of `bridge_attestation_received_without_message_deserializes_to_none`.
    // This guards the backward-compat boundary: switching `message` to a
    // non-optional type, or adding `deny_unknown_fields`, would strand in-flight
    // pre-`message` transfers held in an `Attested` snapshot.
    #[test]
    fn attested_snapshot_without_message_deserializes_to_none() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mut snapshot = to_value(UsdcRebalance::Attested {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            burn_tx_hash: burn_tx,
            cctp_nonce: TEST_CCTP_NONCE,
            attestation: vec![0x01, 0x02, 0x03],
            message: Some(vec![0xDE, 0xAD, 0xBE, 0xEF]),
            mint_scan_from_block: Some(100),
            initiated_at: Utc::now(),
            attested_at: Utc::now(),
        })
        .expect("Attested state serializes");

        // Simulate a snapshot persisted before the `message` field existed.
        snapshot
            .get_mut("Attested")
            .and_then(|attested| attested.as_object_mut())
            .expect("externally-tagged Attested object")
            .remove("message");

        let state =
            from_value::<UsdcRebalance>(snapshot).expect("pre-message snapshot must still load");

        let UsdcRebalance::Attested { message, .. } = state else {
            panic!("expected Attested, got {state:?}");
        };

        assert_eq!(message, None);
    }

    fn conversion(source_amount: Usdc, received_amount: Usdc) -> ConversionAmounts {
        ConversionAmounts::new(source_amount, received_amount)
    }

    fn par_conversion(amount: Usdc) -> ConversionAmounts {
        conversion(amount, amount)
    }

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal: TransferRef::AlpacaId(transfer_id),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected Initiated event");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc::new(float!(1000.00)));
        assert_eq!(*withdrawal_ref, TransferRef::AlpacaId(transfer_id));
    }

    #[tokio::test]
    async fn test_initiate_base_to_alpaca() {
        let tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.50)),
                withdrawal: TransferRef::OnchainTx(tx_hash),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);

        let UsdcRebalanceEvent::Initiated {
            direction,
            amount,
            withdrawal_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected Initiated event");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc::new(float!(500.50)));
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal: TransferRef::AlpacaId(transfer_id),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::AlreadyInitiated)
        ));
    }

    #[test]
    fn non_init_event_on_uninitialized_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
            withdrawal_tx: None,
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn initiated_event_on_withdrawing_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            },
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }

    #[tokio::test]
    async fn begin_withdrawal_from_uninitialized_records_intent_with_chain_head() {
        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::BeginWithdrawal {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalSubmitting {
            direction,
            amount,
            from_block,
            ..
        } = &events[0]
        else {
            panic!("Expected WithdrawalSubmitting event, got {:?}", events[0]);
        };
        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc::new(float!(500.00)));
        assert_eq!(*from_block, 42);
    }

    #[tokio::test]
    async fn begin_withdrawal_from_uninitialized_rejects_alpaca_to_base() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::BeginWithdrawal {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn initiate_from_withdrawal_submitting_advances_to_withdrawing() {
        let tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000abc");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
                submitting_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal: TransferRef::OnchainTx(tx_hash),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::Initiated { withdrawal_ref, .. } = &events[0] else {
            panic!("Expected Initiated event, got {:?}", events[0]);
        };
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn initiate_with_amount_mismatch_from_withdrawal_submitting_errors() {
        let tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000abc");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
                submitting_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(999.00)),
                withdrawal: TransferRef::OnchainTx(tx_hash),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn begin_bridging_from_withdrawal_complete_records_intent_with_chain_head() {
        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging {
                from_block: 99,
                burn_amount: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingSubmitting { from_block, .. } = &events[0] else {
            panic!("Expected BridgingSubmitting event, got {:?}", events[0]);
        };
        assert_eq!(*from_block, 99);
    }

    #[tokio::test]
    async fn initiate_bridging_from_bridging_submitting_records_burn() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000bad");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingSubmitting {
                    from_block: 99,
                    submitting_at: Utc::now(),
                    burn_amount: None,
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated { burn_tx_hash, .. } = &events[0] else {
            panic!("Expected BridgingInitiated event, got {:?}", events[0]);
        };
        assert_eq!(*burn_tx_hash, burn_tx);
    }

    #[tokio::test]
    async fn record_pending_burn_from_bridging_submitting_emits_event_and_sets_pending_tx() {
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000feedface");

        let staged = vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 99,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
        ];

        let events = TestHarness::<UsdcRebalance>::with()
            .given(staged.clone())
            .when(UsdcRebalanceCommand::RecordPendingBurn { burn_tx })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::PendingBurnRecorded {
            burn_tx: recorded, ..
        } = &events[0]
        else {
            panic!("Expected PendingBurnRecorded event, got {:?}", events[0]);
        };
        assert_eq!(*recorded, burn_tx);

        // Replaying the emitted event must leave the aggregate in
        // BridgingSubmitting with pending_burn_tx populated (not advance state).
        let mut replayed = staged;
        replayed.push(events[0].clone());
        let state = replay::<UsdcRebalance>(replayed).unwrap();

        let Some(UsdcRebalance::BridgingSubmitting {
            pending_burn_tx,
            from_block,
            ..
        }) = state
        else {
            panic!("Expected BridgingSubmitting state, got {state:?}");
        };
        assert_eq!(pending_burn_tx, Some(burn_tx));
        assert_eq!(
            from_block, 99,
            "BridgingSubmitting fields must be preserved"
        );
    }

    #[tokio::test]
    async fn second_record_pending_burn_overwrites_the_first_hash() {
        // The revert-retry loop in `burn_recording_pending` records burn1's hash,
        // sees `confirm_burn` revert, then records burn2's hash -- so the
        // aggregate receives TWO `PendingBurnRecorded` events while still in
        // `BridgingSubmitting`. The second hash MUST win: a resume that read the
        // stale first (reverted) hash would see `MinedReverted` and fall through
        // to the scan instead of checking the live second burn.
        let burn_tx_1 =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000011111111");
        let burn_tx_2 =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000022222222");

        let staged = vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 99,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
            UsdcRebalanceEvent::PendingBurnRecorded {
                burn_tx: burn_tx_1,
                recorded_at: Utc::now(),
            },
            // Production always sends `ClearPendingBurn` (which emits `PendingBurnCleared`)
            // BEFORE each (re)broadcast. The second `RecordPendingBurn` therefore arrives
            // after the aggregate's `pending_burn_tx` has been reset to `None`, not
            // directly on top of the first hash. Staging this event here mirrors the
            // actual event stream so the test covers production ordering.
            UsdcRebalanceEvent::PendingBurnCleared {
                cleared_at: Utc::now(),
            },
        ];

        let events = TestHarness::<UsdcRebalance>::with()
            .given(staged.clone())
            .when(UsdcRebalanceCommand::RecordPendingBurn { burn_tx: burn_tx_2 })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::PendingBurnRecorded {
            burn_tx: recorded, ..
        } = &events[0]
        else {
            panic!("Expected PendingBurnRecorded event, got {:?}", events[0]);
        };
        assert_eq!(*recorded, burn_tx_2);

        let mut replayed = staged;
        replayed.push(events[0].clone());
        let state = replay::<UsdcRebalance>(replayed).unwrap();

        let Some(UsdcRebalance::BridgingSubmitting {
            pending_burn_tx, ..
        }) = state
        else {
            panic!("Expected BridgingSubmitting state, got {state:?}");
        };
        assert_eq!(
            pending_burn_tx,
            Some(burn_tx_2),
            "the second RecordPendingBurn must overwrite the first hash, not be ignored"
        );
    }

    #[tokio::test]
    async fn record_pending_burn_from_non_bridging_submitting_state_errors() {
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000feedface");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::RecordPendingBurn { burn_tx })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotConfirmed)
            ),
            "RecordPendingBurn from WithdrawalComplete must error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn clear_pending_burn_resets_pending_tx_to_none() {
        // A reburn clears the stale recorded hash BEFORE re-broadcasting. The
        // aggregate is in `BridgingSubmitting` carrying a recorded burn hash;
        // `ClearPendingBurn` must emit one `PendingBurnCleared` event that resets
        // `pending_burn_tx` to None, so a failed re-record fails closed instead of
        // reburning off the stale (reverted) hash.
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000011111111");

        let staged = vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 99,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
            UsdcRebalanceEvent::PendingBurnRecorded {
                burn_tx,
                recorded_at: Utc::now(),
            },
        ];

        let events = TestHarness::<UsdcRebalance>::with()
            .given(staged.clone())
            .when(UsdcRebalanceCommand::ClearPendingBurn)
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::PendingBurnCleared { .. } = &events[0] else {
            panic!("Expected PendingBurnCleared event, got {:?}", events[0]);
        };

        let mut replayed = staged;
        replayed.push(events[0].clone());
        let state = replay::<UsdcRebalance>(replayed).unwrap();

        let Some(UsdcRebalance::BridgingSubmitting {
            pending_burn_tx,
            from_block,
            ..
        }) = state
        else {
            panic!("Expected BridgingSubmitting state, got {state:?}");
        };
        assert_eq!(
            pending_burn_tx, None,
            "ClearPendingBurn must reset pending_burn_tx to None"
        );
        assert_eq!(
            from_block, 99,
            "BridgingSubmitting fields must be preserved"
        );
    }

    #[tokio::test]
    async fn clear_pending_burn_when_already_none_is_noop() {
        // On the FIRST burn there is no recorded hash yet, so `ClearPendingBurn`
        // must be a no-op that emits ZERO events rather than churning the stream.
        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingSubmitting {
                    from_block: 99,
                    submitting_at: Utc::now(),
                    burn_amount: None,
                },
            ])
            .when(UsdcRebalanceCommand::ClearPendingBurn)
            .await
            .events();

        assert_eq!(
            events.len(),
            0,
            "ClearPendingBurn with no recorded hash must emit no events, got {events:?}"
        );
    }

    #[tokio::test]
    async fn clear_pending_burn_from_non_bridging_submitting_errors() {
        // Once the burn is broadcast and the aggregate has advanced to `Bridging`,
        // clearing the pending burn is invalid: the bridge is already underway.
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000feedface");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ClearPendingBurn)
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyStarted)
            ),
            "ClearPendingBurn from Bridging must error, got {error:?}"
        );
    }

    #[tokio::test]
    async fn record_pending_burn_from_bridging_errors() {
        // Once the burn is broadcast and the aggregate has advanced to `Bridging`,
        // recording a pending burn is invalid: the burn is settled and the
        // pending-burn slot no longer exists.
        let burn_tx =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000feedface");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::RecordPendingBurn { burn_tx })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyStarted)
            ),
            "RecordPendingBurn from Bridging must error, got {error:?}"
        );
    }

    /// A `BridgingSubmitting` snapshot persisted before `pending_burn_tx` existed
    /// must still load, defaulting the field to `None` via `#[serde(default)]`.
    #[test]
    fn bridging_submitting_snapshot_without_pending_burn_tx_deserializes_to_none() {
        let mut snapshot = to_value(UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100.00)),
            from_block: 42,
            initiated_at: Utc::now(),
            burn_amount: None,
            pending_burn_tx: Some(fixed_bytes!(
                "0x00000000000000000000000000000000000000000000000000000000feedface"
            )),
        })
        .expect("BridgingSubmitting state serializes");

        snapshot
            .get_mut("BridgingSubmitting")
            .and_then(|value| value.as_object_mut())
            .expect("externally-tagged BridgingSubmitting object")
            .remove("pending_burn_tx");

        let state = from_value::<UsdcRebalance>(snapshot)
            .expect("pre-field BridgingSubmitting snapshot must still load");

        let UsdcRebalance::BridgingSubmitting {
            pending_burn_tx, ..
        } = state
        else {
            panic!("expected BridgingSubmitting, got {state:?}");
        };
        assert_eq!(pending_burn_tx, None);
    }

    #[test]
    fn intent_first_event_sequence_replays_through_withdrawal_and_bridging() {
        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
                submitting_at: Utc::now(),
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 99,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
        ])
        .unwrap();

        assert!(
            matches!(state, Some(UsdcRebalance::BridgingSubmitting { from_block, .. }) if from_block == 99),
            "Expected BridgingSubmitting state after intent-first sequence, got {state:?}"
        );
    }

    /// Hypothesis: `BeginBridging { burn_amount: Some(received) }` emits a
    /// `BridgingSubmitting` event that carries `burn_amount: Some(received)`,
    /// and replaying that event produces a `BridgingSubmitting` aggregate state
    /// that also carries `burn_amount: Some(received)`.
    #[tokio::test]
    async fn begin_bridging_with_some_burn_amount_propagates_through_event_and_state() {
        let received = Usdc::new(float!(998.00));

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging {
                from_block: 42,
                burn_amount: Some(received),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingSubmitting {
            from_block,
            burn_amount,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingSubmitting event, got {:?}", events[0]);
        };
        assert_eq!(*from_block, 42);
        assert_eq!(
            *burn_amount,
            Some(received),
            "BridgingSubmitting event must carry burn_amount: Some(received)"
        );

        // Replay the emitted event and verify the aggregate state matches.
        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 42,
                submitting_at: Utc::now(),
                burn_amount: Some(received),
            },
        ])
        .unwrap();

        let Some(UsdcRebalance::BridgingSubmitting {
            burn_amount: state_burn_amount,
            ..
        }) = state
        else {
            panic!("Expected BridgingSubmitting state; got: {state:?}");
        };
        assert_eq!(
            state_burn_amount,
            Some(received),
            "Replayed BridgingSubmitting state must carry burn_amount: Some(received)"
        );
    }

    /// After the burn is initiated, the `Bridging` state must carry the actual
    /// burned amount (received after fees), not the nominal -- post-burn DTOs and
    /// reconciliation downstream read `Bridging.amount`.
    #[test]
    fn bridging_state_carries_actual_burn_amount_not_nominal() {
        let nominal = Usdc::new(float!(1000.00));
        let received = Usdc::new(float!(998.00));

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: nominal,
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 42,
                submitting_at: Utc::now(),
                burn_amount: Some(received),
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::ZERO,
                burned_at: Utc::now(),
            },
        ])
        .unwrap();

        let Some(UsdcRebalance::Bridging { amount, .. }) = state else {
            panic!("Expected Bridging state; got: {state:?}");
        };
        assert_eq!(
            amount, received,
            "Bridging.amount must be the actual burned amount, not the nominal"
        );
    }

    /// A legacy `BridgingSubmitting` with `burn_amount: None` falls back to the
    /// nominal amount when entering `Bridging`.
    #[test]
    fn bridging_state_falls_back_to_nominal_when_burn_amount_absent() {
        let nominal = Usdc::new(float!(1000.00));

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: nominal,
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 42,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: TxHash::ZERO,
                burned_at: Utc::now(),
            },
        ])
        .unwrap();

        let Some(UsdcRebalance::Bridging { amount, .. }) = state else {
            panic!("Expected Bridging state; got: {state:?}");
        };
        assert_eq!(
            amount, nominal,
            "legacy burn_amount: None must use the nominal"
        );
    }

    /// `BeginBridging` with a burn amount above the nominal is rejected before
    /// any `BridgingSubmitting` event is persisted.
    #[tokio::test]
    async fn begin_bridging_rejects_burn_amount_above_nominal() {
        let nominal = Usdc::new(float!(1000.00));
        let above = Usdc::new(float!(1000.01));

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: nominal,
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging {
                from_block: 42,
                burn_amount: Some(above),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(UsdcRebalanceError::InvalidBurnAmount { burn_amount, nominal: n })
                    if burn_amount == above && n == nominal
            ),
            "expected InvalidBurnAmount for above-nominal burn, got: {error:?}"
        );
    }

    /// `BeginBridging` with a zero burn amount is rejected before any
    /// `BridgingSubmitting` event is persisted.
    #[tokio::test]
    async fn begin_bridging_rejects_zero_burn_amount() {
        let nominal = Usdc::new(float!(1000.00));
        let zero = Usdc::new(float!(0));

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: nominal,
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging {
                from_block: 42,
                burn_amount: Some(zero),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                error,
                LifecycleError::Apply(UsdcRebalanceError::InvalidBurnAmount { burn_amount, nominal: n })
                    if burn_amount == zero && n == nominal
            ),
            "expected InvalidBurnAmount for zero burn, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_confirm_withdrawal() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::WithdrawalConfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_before_initiating() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_twice() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_fail_withdrawal_after_initiation() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::FailWithdrawal {
                reason: "Insufficient funds".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalFailed { reason, .. } = &events[0] else {
            panic!("Expected WithdrawalFailed event");
        };
        assert_eq!(reason, "Insufficient funds");
    }

    #[tokio::test]
    async fn test_cannot_fail_withdrawal_before_initiating() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::FailWithdrawal {
                reason: "Test failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_confirmed_withdrawal() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::FailWithdrawal {
                reason: "Late failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_failed_withdrawal() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "First failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailWithdrawal {
                reason: "Second failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_initiate_bridging_after_withdrawal_confirmed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_tx_hash,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: event_tx_hash,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingInitiated event");
        };

        assert_eq!(*event_tx_hash, burn_tx_hash);
    }

    #[tokio::test]
    async fn test_timeout_attestation_after_bridging_initiated() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let retry_deadline_at = Utc::now() + chrono::Duration::hours(24);

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::TimeoutAttestation { retry_deadline_at })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::AttestationTimedOut {
            burn_tx_hash: event_burn_tx_hash,
            retry_deadline_at: event_retry_deadline_at,
            ..
        } = events[0]
        else {
            panic!("Expected AttestationTimedOut event");
        };
        assert_eq!(event_burn_tx_hash, burn_tx_hash);
        assert_eq!(event_retry_deadline_at, retry_deadline_at);
    }

    #[test]
    fn replay_timeout_attestation_enters_awaiting_attestation() {
        let initiated_at = Utc::now();
        let timed_out_at = initiated_at + chrono::Duration::minutes(5);
        let retry_deadline_at = initiated_at + chrono::Duration::hours(24);

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::OnchainTx(BURN_TX),
                initiated_at,
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: initiated_at,
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: BURN_TX,
                burned_at: initiated_at,
            },
            UsdcRebalanceEvent::AttestationTimedOut {
                burn_tx_hash: BURN_TX,
                retry_deadline_at,
                timed_out_at,
            },
        ])
        .expect("event stream should replay")
        .expect("event stream should materialize aggregate state");

        assert!(
            matches!(
                state,
                UsdcRebalance::AwaitingAttestation {
                    direction: RebalanceDirection::BaseToAlpaca,
                    burn_tx_hash: BURN_TX,
                    retry_deadline_at: deadline,
                    timed_out_at: timeout,
                    ..
                } if deadline == retry_deadline_at && timeout == timed_out_at
            ),
            "Expected AwaitingAttestation state, got {state:?}"
        );
    }

    #[test]
    fn receive_attestation_after_timeout_advances_to_attested() {
        let initiated_at = Utc::now();
        let attested_at = initiated_at + chrono::Duration::minutes(6);

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::OnchainTx(BURN_TX),
                initiated_at,
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: initiated_at,
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: BURN_TX,
                burned_at: initiated_at,
            },
            UsdcRebalanceEvent::AttestationTimedOut {
                burn_tx_hash: BURN_TX,
                retry_deadline_at: initiated_at + chrono::Duration::hours(24),
                timed_out_at: initiated_at + chrono::Duration::minutes(5),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01, 0x02],
                cctp_nonce: TEST_CCTP_NONCE,
                message: None,
                mint_scan_from_block: Some(100),
                attested_at,
            },
        ])
        .expect("event stream should replay")
        .expect("event stream should materialize aggregate state");

        assert!(
            matches!(
                state,
                UsdcRebalance::Attested {
                    burn_tx_hash: BURN_TX,
                    cctp_nonce: TEST_CCTP_NONCE,
                    attested_at: state_attested_at,
                    ..
                } if state_attested_at == attested_at
            ),
            "Expected Attested state, got {state:?}"
        );
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_before_withdrawal() {
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_tx_hash,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotConfirmed)
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_while_withdrawing() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_tx_hash,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotConfirmed)
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_after_withdrawal_failed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "Test failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_tx_hash,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotConfirmed)
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_twice() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging {
                burn_tx: burn_tx_hash,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn test_receive_attestation_after_bridging_initiated() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let attestation = vec![0x01, 0x02, 0x03, 0x04];
        let message = vec![0x05, 0x06, 0x07, 0x08];

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: attestation.clone(),
                cctp_nonce: TEST_CCTP_NONCE,
                message: message.clone(),
                mint_scan_from_block: 8_675_309,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: event_attestation,
            cctp_nonce: event_nonce,
            message: event_message,
            mint_scan_from_block,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgeAttestationReceived event");
        };

        assert_eq!(*event_attestation, attestation);
        assert_eq!(
            *event_message,
            Some(message),
            "ReceiveAttestation must persist the full CCTP message envelope into the event",
        );
        assert_eq!(
            *mint_scan_from_block,
            Some(8_675_309),
            "ReceiveAttestation must persist the destination-chain scan bound into the event",
        );
        // Regression guard for RAI-714: the full 32-byte nonce must survive
        // unchanged through ReceiveAttestation, not be truncated/zeroed.
        assert_eq!(*event_nonce, TEST_CCTP_NONCE);
    }

    #[test]
    fn evolve_carries_mint_scan_from_block_into_attested() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let initiated_at = Utc::now();
        let bridging = UsdcRebalance::Bridging {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(1000)),
            burn_tx_hash: burn_tx,
            initiated_at,
            burned_at: initiated_at,
        };

        let attested = UsdcRebalance::evolve(
            &bridging,
            &UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: None,
                mint_scan_from_block: Some(8_675_309),
                attested_at: Utc::now(),
            },
        )
        .unwrap()
        .expect("Bridging must evolve to Attested on BridgeAttestationReceived");

        let UsdcRebalance::Attested {
            mint_scan_from_block,
            ..
        } = attested
        else {
            panic!("expected Attested state");
        };

        assert_eq!(
            mint_scan_from_block,
            Some(8_675_309),
            "evolve must carry the scan bound into the Attested state",
        );
    }

    // `resumable_post_burn_transfer` is the classifier startup recovery uses to
    // decide which stranded aggregates get a re-armed transfer job. Pin all three
    // post-burn resumable arms (carrying the right direction so the re-arm routes
    // to the correct queue) and a terminal arm that must NOT be re-armed -- a
    // narrowed match would silently strand or double-drive burned-USDC transfers.
    #[test]
    fn resumable_post_burn_transfer_classifies_post_burn_states() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let now = Utc::now();
        let amount = Usdc::new(float!(750));

        let bridging = UsdcRebalance::Bridging {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            burn_tx_hash: burn_tx,
            initiated_at: now,
            burned_at: now,
        };
        assert_eq!(
            bridging.resumable_post_burn_transfer(),
            Some((RebalanceDirection::BaseToAlpaca, amount)),
            "Bridging is post-burn and must be resumable",
        );

        let awaiting = UsdcRebalance::AwaitingAttestation {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            burn_tx_hash: burn_tx,
            initiated_at: now,
            timed_out_at: now,
            retry_deadline_at: now,
        };
        assert_eq!(
            awaiting.resumable_post_burn_transfer(),
            Some((RebalanceDirection::AlpacaToBase, amount)),
            "AwaitingAttestation is post-burn and must be resumable in its own direction",
        );

        let attested = UsdcRebalance::Attested {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            burn_tx_hash: burn_tx,
            cctp_nonce: TEST_CCTP_NONCE,
            attestation: vec![0x01],
            message: Some(vec![0xDE, 0xAD]),
            mint_scan_from_block: Some(100),
            initiated_at: now,
            attested_at: now,
        };
        assert_eq!(
            attested.resumable_post_burn_transfer(),
            Some((RebalanceDirection::AlpacaToBase, amount)),
            "Attested is post-burn, pre-mint and must be resumable",
        );

        // A confirmed mint is terminal for the transfer leg: re-arming it would
        // double-drive an already-bridged transfer, so it must classify as None.
        let bridged = UsdcRebalance::Bridged {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            amount_received: amount,
            fee_collected: Usdc::new(float!(0.01)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: burn_tx,
            initiated_at: now,
            minted_at: now,
        };
        assert_eq!(
            bridged.resumable_post_burn_transfer(),
            None,
            "Bridged (mint confirmed) must not be re-armed",
        );

        // BridgingSubmitting is the pre-burn intent marker: the on-chain burn may
        // or may not have landed. Re-arming it could drive a second CCTP burn, so
        // it must classify as None and be left to crash-safe scan/operator
        // recovery -- never auto-re-armed.
        let submitting = UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            from_block: 100,
            initiated_at: now,
            burn_amount: None,
            pending_burn_tx: None,
        };
        assert_eq!(
            submitting.resumable_post_burn_transfer(),
            None,
            "BridgingSubmitting is pre-burn and must NOT be re-armed (double-burn risk)",
        );

        // A post-burn BaseToAlpaca BridgingFailed is recoverable (RAI-906): the
        // resume path re-checks the mint and un-fails it, so it must be re-armed.
        let post_burn_failed = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            burn_tx_hash: Some(burn_tx),
            cctp_nonce: Some(TEST_CCTP_NONCE),
            reason: "dropped from mempool".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(
            post_burn_failed.resumable_post_burn_transfer(),
            Some((RebalanceDirection::BaseToAlpaca, amount)),
            "post-burn BaseToAlpaca BridgingFailed is recoverable and must be re-armed",
        );

        // A pre-burn failure has no mint to adopt -- not re-armable.
        let pre_burn_failed = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "pre-burn failure".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(
            pre_burn_failed.resumable_post_burn_transfer(),
            None,
            "pre-burn BridgingFailed has no mint to recover and must NOT be re-armed",
        );

        // AlpacaToBase recovery is not yet implemented -- left for manual
        // reconciliation rather than auto-re-armed into an unsupported path.
        let alpaca_to_base_failed = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            burn_tx_hash: Some(burn_tx),
            cctp_nonce: Some(TEST_CCTP_NONCE),
            reason: "dropped from mempool".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(
            alpaca_to_base_failed.resumable_post_burn_transfer(),
            None,
            "AlpacaToBase BridgingFailed recovery is unimplemented and must NOT be re-armed",
        );
    }

    // `direction` backs the CLI resume guard's `--direction` validation, so a
    // wrong destructure in its 16-arm match would mis-classify a transfer's
    // direction and either reject a valid resume or mis-drive the aggregate.
    // Spot-check states spread across the or-pattern in both directions.
    #[test]
    fn direction_reads_persisted_direction() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let now = Utc::now();
        let amount = Usdc::new(float!(321));

        let bridged = UsdcRebalance::Bridged {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            amount_received: Usdc::new(float!(320)),
            fee_collected: Usdc::new(float!(1)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: burn_tx,
            initiated_at: now,
            minted_at: now,
        };
        assert_eq!(bridged.direction(), RebalanceDirection::AlpacaToBase);

        let conversion_complete = UsdcRebalance::ConversionComplete {
            direction: RebalanceDirection::BaseToAlpaca,
            amount,
            conversion: par_conversion(Usdc::new(float!(999))),
            initiated_at: now,
            converted_at: now,
        };
        assert_eq!(
            conversion_complete.direction(),
            RebalanceDirection::BaseToAlpaca,
        );

        let deposit_failed = UsdcRebalance::DepositFailed {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            burn_tx_hash: burn_tx,
            mint_tx_hash: burn_tx,
            deposit_ref: None,
            reason: "boom".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(deposit_failed.direction(), RebalanceDirection::AlpacaToBase);
    }

    /// `amount()` must return the persisted EFFECTIVE amount: the conversion
    /// fill once a conversion completed, the post-withdrawal-fee burn amount once
    /// a `BridgingSubmitting` carries one, and the requested amount otherwise.
    #[test]
    fn amount_reads_persisted_effective_amount() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let now = Utc::now();
        let requested = Usdc::new(float!(321));

        let bridged = UsdcRebalance::Bridged {
            direction: RebalanceDirection::AlpacaToBase,
            amount: requested,
            amount_received: Usdc::new(float!(320)),
            fee_collected: Usdc::new(float!(1)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: burn_tx,
            initiated_at: now,
            minted_at: now,
        };
        assert_eq!(
            bridged.amount(),
            Usdc::new(float!(320)),
            "post-bridge the received amount is the effective amount",
        );

        let conversion_failed = UsdcRebalance::ConversionFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: requested,
            order_id: ClientOrderId::from_uuid(Uuid::from_u128(11)),
            reason: "broker rejected".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(conversion_failed.amount(), requested);

        let conversion_complete = UsdcRebalance::ConversionComplete {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: requested,
            conversion: par_conversion(Usdc::new(float!(319))),
            initiated_at: now,
            converted_at: now,
        };
        assert_eq!(
            conversion_complete.amount(),
            Usdc::new(float!(319)),
            "post-conversion the filled amount is the effective amount",
        );

        let deposit_failed = UsdcRebalance::DepositFailed {
            direction: RebalanceDirection::AlpacaToBase,
            amount: requested,
            burn_tx_hash: burn_tx,
            mint_tx_hash: burn_tx,
            deposit_ref: None,
            reason: "deposit reverted".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        assert_eq!(
            deposit_failed.amount(),
            requested,
            "the common reconcile target state reports the requested amount",
        );

        let bridging_submitting_with_burn = UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: requested,
            from_block: 100,
            initiated_at: now,
            burn_amount: Some(Usdc::new(float!(318))),
            pending_burn_tx: None,
        };
        assert_eq!(
            bridging_submitting_with_burn.amount(),
            Usdc::new(float!(318)),
            "BridgingSubmitting reports the persisted burn_amount the manager resumes with",
        );

        let bridging_submitting_legacy = UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: requested,
            from_block: 100,
            initiated_at: now,
            burn_amount: None,
            pending_burn_tx: None,
        };
        assert_eq!(
            bridging_submitting_legacy.amount(),
            requested,
            "BridgingSubmitting without a persisted burn_amount falls back to the nominal",
        );

        let converting = UsdcRebalance::Converting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: requested,
            order_id: ClientOrderId::from_uuid(Uuid::from_u128(12)),
            initiated_at: now,
        };
        assert_eq!(
            converting.amount(),
            requested,
            "Converting reports the requested amount until a conversion completes",
        );

        let bridging = UsdcRebalance::Bridging {
            direction: RebalanceDirection::AlpacaToBase,
            amount: requested,
            burn_tx_hash: burn_tx,
            initiated_at: now,
            burned_at: now,
        };
        assert_eq!(
            bridging.amount(),
            requested,
            "Bridging reports the requested amount until the mint lands",
        );
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_before_bridging() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_while_withdrawing() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_complete() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_failed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "Test failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_twice() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x03, 0x04],
                cctp_nonce: OTHER_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn test_confirm_bridging() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02, 0x03, 0x04],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx: mint_tx_hash,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::Bridged {
            mint_tx_hash: event_mint_tx,
            ..
        } = &events[0]
        else {
            panic!("Expected Bridged event");
        };

        assert_eq!(*event_mint_tx, mint_tx_hash);
    }

    #[tokio::test]
    async fn test_cannot_confirm_bridging_before_attestation() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx: mint_tx_hash,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::AttestationNotReceived)
        ));
    }

    #[tokio::test]
    async fn test_fail_bridging_from_withdrawal_complete() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "USDC conversion failed".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash,
            cctp_nonce,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingFailed event");
        };

        assert_eq!(*burn_tx_hash, None);
        assert_eq!(*cctp_nonce, None);
        assert_eq!(reason, "USDC conversion failed");
    }

    #[tokio::test]
    async fn test_fail_bridging_after_initiated() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "CCTP timeout".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: event_burn_tx,
            cctp_nonce: event_nonce,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingFailed event");
        };

        assert_eq!(*event_burn_tx, Some(burn_tx_hash));
        assert_eq!(*event_nonce, None);
        assert_eq!(reason, "CCTP timeout");
    }

    /// Failing from `AwaitingAttestation` (reached after an attestation timeout,
    /// e.g. when the retry deadline elapses) must retain `burn_tx_hash` with a
    /// `None` nonce. That populated burn tx is what keeps `holds_rebalance_guard`
    /// latched on restart -- a regression to the pre-burn shape would clear the
    /// guard and reopen the re-burn window on already-burned USDC.
    #[tokio::test]
    async fn test_fail_bridging_from_awaiting_attestation() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::AttestationTimedOut {
                    burn_tx_hash,
                    retry_deadline_at: Utc::now() + chrono::Duration::hours(1),
                    timed_out_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "attestation retry deadline elapsed".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: event_burn_tx,
            cctp_nonce: event_nonce,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingFailed event");
        };

        assert_eq!(*event_burn_tx, Some(burn_tx_hash));
        assert_eq!(*event_nonce, None);
        assert_eq!(reason, "attestation retry deadline elapsed");
    }

    #[tokio::test]
    async fn test_fail_bridging_after_attestation_received() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = TEST_CCTP_NONCE;

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "Mint transaction failed".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: event_burn_tx,
            cctp_nonce: event_nonce,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingFailed event");
        };

        assert_eq!(*event_burn_tx, Some(burn_tx_hash));
        assert_eq!(*event_nonce, Some(cctp_nonce));
        assert_eq!(reason, "Mint transaction failed");
    }

    #[tokio::test]
    async fn test_bridging_failed_preserves_burn_data_when_available() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0xabababababababababababababababababababababababababababababababab");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "Attestation service down".to_string(),
            })
            .await
            .events();

        let UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: event_burn_tx,
            cctp_nonce: event_nonce,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingFailed event");
        };

        assert_eq!(
            *event_burn_tx,
            Some(burn_tx_hash),
            "Burn tx hash should be preserved in failure event"
        );
        assert_eq!(
            *event_nonce, None,
            "CCTP nonce should be None when failing from Bridging state"
        );
    }

    #[tokio::test]
    async fn test_cannot_confirm_bridging_after_already_completed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx: fixed_bytes!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_bridging_after_already_completed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "Late failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_bridging_after_already_failed() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: Some(burn_tx_hash),
                    cctp_nonce: None,
                    reason: "First failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailBridging {
                reason: "Second failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_initiate_deposit_with_alpaca_transfer() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateDeposit {
                deposit: deposit_ref.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositInitiated event");
        };

        assert_eq!(*event_deposit_ref, deposit_ref);
    }

    #[tokio::test]
    async fn test_initiate_deposit_with_onchain_tx() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let deposit_tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let deposit_ref = TransferRef::OnchainTx(deposit_tx_hash);

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateDeposit {
                deposit: deposit_ref.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositInitiated event");
        };

        assert_eq!(*event_deposit_ref, deposit_ref);
    }

    #[tokio::test]
    async fn test_cannot_deposit_before_bridging_complete() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateDeposit {
                deposit: TransferRef::AlpacaId(deposit_transfer_id),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingNotCompleted)
        ));
    }

    #[tokio::test]
    async fn test_confirm_deposit() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref,
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::DepositConfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_deposit_before_initiating() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::DepositNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_fail_deposit_after_initiated() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: deposit_ref.clone(),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailDeposit {
                reason: "Test failure reason".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositFailed {
            deposit_ref: event_deposit_ref,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositFailed event");
        };

        assert_eq!(*event_deposit_ref, Some(deposit_ref));
        assert_eq!(reason, "Test failure reason");
    }

    #[tokio::test]
    async fn test_deposit_failed_preserves_deposit_ref_when_available() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let onchain_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let deposit_ref = TransferRef::OnchainTx(onchain_tx);

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: deposit_ref.clone(),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailDeposit {
                reason: "Onchain deposit failed".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositFailed {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositFailed event");
        };

        assert_eq!(*event_deposit_ref, Some(deposit_ref));
    }

    #[tokio::test]
    async fn test_complete_alpaca_to_base_full_flow() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let deposit_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(10000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0xAB, 0xCD, 0xEF],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(deposit_tx),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::DepositConfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn test_complete_base_to_alpaca_full_flow() {
        let withdrawal_tx =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");
        let burn_tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");
        let mint_tx_hash =
            fixed_bytes!("0x5555555555555555555555555555555555555555555555555555555555555555");
        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(5000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(withdrawal_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x11, 0x22, 0x33, 0x44],
                    cctp_nonce: OTHER_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(deposit_transfer_id),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::DepositConfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn test_withdrawal_failed_rejects_confirm_withdrawal() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "Test failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_withdrawal_failed_rejects_initiate_bridging() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalFailed {
                    reason: "Test failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WithdrawalNotConfirmed)
        ));
    }

    #[tokio::test]
    async fn test_bridging_failed_rejects_receive_attestation() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: Some(burn_tx),
                    cctp_nonce: None,
                    reason: "Bridge failed".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![],
                mint_scan_from_block: 100,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_bridging_failed_rejects_confirm_bridging() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: Some(burn_tx),
                    cctp_nonce: None,
                    reason: "Bridge failed".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::BridgingAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn recover_bridging_unfails_post_burn_failed_to_bridged() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let nonce =
            fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000aa");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // A post-burn BridgingFailed (carries a burn tx) whose mint in fact
        // landed: RecoverBridging un-fails it back to Bridged.
        let history = vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(burn_tx),
                cctp_nonce: Some(nonce),
                reason: "dropped from mempool".to_string(),
                failed_at: Utc::now(),
            },
        ];

        let events = TestHarness::<UsdcRebalance>::with()
            .given(history.clone())
            .when(UsdcRebalanceCommand::RecoverBridging {
                mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingCompletionRecovered {
            mint_tx_hash,
            amount_received,
            fee_collected,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingCompletionRecovered, got {:?}", events[0]);
        };
        assert_eq!(*mint_tx_hash, mint_tx);
        assert_eq!(*amount_received, Usdc::new(float!(99.99)));
        assert_eq!(*fee_collected, Usdc::new(float!(0.01)));

        // Applying the event must un-fail the aggregate back to Bridged with the
        // recovered mint adopted -- the apply arm, not just the command handler.
        let state = replay::<UsdcRebalance>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize aggregate state");
        assert!(
            matches!(
                state,
                UsdcRebalance::Bridged {
                    direction: RebalanceDirection::BaseToAlpaca,
                    burn_tx_hash: state_burn_tx,
                    mint_tx_hash: state_mint_tx,
                    amount_received: state_received,
                    fee_collected: state_fee,
                    ..
                } if state_burn_tx == burn_tx
                    && state_mint_tx == mint_tx
                    && state_received == Usdc::new(float!(99.99))
                    && state_fee == Usdc::new(float!(0.01))
            ),
            "recovered aggregate should be Bridged with the adopted mint, got {state:?}"
        );
    }

    #[tokio::test]
    async fn recover_bridging_rejects_pre_burn_failed() {
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // A pre-burn BridgingFailed (no burn tx) has no mint to adopt; recovery
        // must be rejected -- the transfer should be re-initiated, not un-failed.
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: None,
                    cctp_nonce: None,
                    reason: "pre-burn failure".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::RecoverBridging {
                mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn recover_bridging_rejects_non_failed_state() {
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        // RecoverBridging is only valid from a post-burn BridgingFailed. From a
        // non-failed state (here mid-bridge, after the burn) it must be rejected
        // as an invalid command, never silently adopting a second mint.
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    )),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: fixed_bytes!(
                        "0x0000000000000000000000000000000000000000000000000000000000000001"
                    ),
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::RecoverBridging {
                mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    /// Event history that lands a BaseToAlpaca aggregate in the post-burn
    /// terminal `DepositFailed` state, for the operator-reconcile tests.
    fn deposit_failed_history() -> Vec<UsdcRebalanceEvent> {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: None,
                mint_scan_from_block: Some(100),
                attested_at: Utc::now(),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
                minted_at: Utc::now(),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                deposit_initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref: Some(TransferRef::OnchainTx(mint_tx)),
                reason: "deposit rejected".to_string(),
                failed_at: Utc::now(),
            },
        ]
    }

    /// Event history that lands a BaseToAlpaca aggregate in a post-burn terminal
    /// `BridgingFailed` state (a burn tx is recorded but the bridge never
    /// completed), for the operator-reconcile tests.
    fn post_burn_bridging_failed_history() -> Vec<UsdcRebalanceEvent> {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            },
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(burn_tx),
                cctp_nonce: None,
                reason: "attestation never arrived".to_string(),
                failed_at: Utc::now(),
            },
        ]
    }

    /// Event history that lands a BaseToAlpaca aggregate in the post-deposit
    /// (post-mint) terminal `ConversionFailed` state -- the USDC->USD leg failed
    /// after the deposit confirmed -- for the operator-reconcile tests.
    fn base_to_alpaca_conversion_failed_history() -> Vec<UsdcRebalanceEvent> {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: None,
                mint_scan_from_block: Some(100),
                attested_at: Utc::now(),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: mint_tx,
                amount_received: Usdc::new(float!(99.99)),
                fee_collected: Usdc::new(float!(0.01)),
                minted_at: Utc::now(),
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                deposit_initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                deposit_confirmed_at: Utc::now(),
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(99.99)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::ConversionFailed {
                reason: "USDC->USD conversion rejected".to_string(),
                failed_at: Utc::now(),
            },
        ]
    }

    /// Drives `ReconcileStuckRebalance` against a history that ends in a
    /// guard-stranding post-burn failure and asserts it emits a single
    /// `OperatorReconciled` preserving the source amount and initiation
    /// timestamp, then materializes the clearing terminal `Reconciled` with
    /// the source failure reason recovered from the prior state.
    async fn assert_reconcile_emits_operator_reconciled(
        history: Vec<UsdcRebalanceEvent>,
        expected_direction: RebalanceDirection,
    ) {
        // The source failure carries the real post-burn amount, original
        // initiation timestamp, and failure reason; reconciliation must
        // preserve all three so the projection does not collapse the transfer
        // to a zero-value one and does not lose the failure context.
        let (source_amount, source_initiated_at, source_failure_reason) =
            match replay::<UsdcRebalance>(history.clone())
                .expect("history should replay")
                .expect("history should materialize a failure state")
            {
                UsdcRebalance::DepositFailed {
                    amount,
                    initiated_at,
                    reason,
                    ..
                }
                | UsdcRebalance::BridgingFailed {
                    amount,
                    initiated_at,
                    reason,
                    ..
                }
                | UsdcRebalance::ConversionFailed {
                    amount,
                    initiated_at,
                    reason,
                    ..
                } => (amount, initiated_at, reason),
                other => panic!("fixture should end in a post-burn failure, got {other:?}"),
            };

        let events = TestHarness::<UsdcRebalance>::with()
            .given(history.clone())
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::OperatorReconciled {
            direction,
            amount,
            reason,
            initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected OperatorReconciled, got {:?}", events[0]);
        };
        assert_eq!(*direction, expected_direction);
        assert_eq!(*amount, source_amount);
        assert_eq!(*reason, ReconcileReason::FundsMovedManually);
        assert_eq!(*initiated_at, source_initiated_at);

        // Applying the event must drive the aggregate to the clearing terminal
        // Reconciled state with failure_reason recovered from the prior state.
        let state = replay::<UsdcRebalance>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize aggregate state");
        let UsdcRebalance::Reconciled { failure_reason, .. } = state else {
            panic!("reconciled aggregate should be Reconciled, got {state:?}");
        };
        assert_eq!(
            failure_reason,
            Some(source_failure_reason),
            "reconciled state must carry the source failure reason"
        );
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_from_post_burn_bridging_failed_emits_operator_reconciled() {
        assert_reconcile_emits_operator_reconciled(
            post_burn_bridging_failed_history(),
            RebalanceDirection::BaseToAlpaca,
        )
        .await;
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_from_base_to_alpaca_conversion_failed_emits_reconciled() {
        assert_reconcile_emits_operator_reconciled(
            base_to_alpaca_conversion_failed_history(),
            RebalanceDirection::BaseToAlpaca,
        )
        .await;
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_from_deposit_failed_emits_operator_reconciled() {
        let history = deposit_failed_history();

        let events = TestHarness::<UsdcRebalance>::with()
            .given(history.clone())
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .events();

        // The DepositFailed source state carries the real post-burn amount,
        // original initiation timestamp, and failure reason; reconciliation
        // must preserve all three so the projection does not collapse the
        // transfer to a zero-value one and does not lose the failure context.
        let UsdcRebalance::DepositFailed {
            amount: failed_amount,
            initiated_at: failed_initiated_at,
            reason: failed_reason,
            ..
        } = replay::<UsdcRebalance>(history.clone())
            .expect("history should replay")
            .expect("history should materialize DepositFailed")
        else {
            panic!("fixture should end in DepositFailed");
        };

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::OperatorReconciled {
            direction,
            amount,
            reason,
            initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected OperatorReconciled, got {:?}", events[0]);
        };
        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, failed_amount);
        assert_eq!(*reason, ReconcileReason::FundsMovedManually);
        assert_eq!(*initiated_at, failed_initiated_at);

        // Applying the event must drive the aggregate to the clearing terminal
        // Reconciled state with failure_reason recovered from the prior state.
        let state = replay::<UsdcRebalance>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize aggregate state");
        let UsdcRebalance::Reconciled {
            direction: reconciled_direction,
            reason: reconciled_reason,
            failure_reason,
            ..
        } = state
        else {
            panic!("reconciled aggregate should be Reconciled, got {state:?}");
        };
        assert_eq!(reconciled_direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(reconciled_reason, ReconcileReason::FundsMovedManually);
        assert_eq!(
            failure_reason,
            Some(failed_reason),
            "reconciled state must carry the source failure reason"
        );
    }

    #[test]
    fn reconciled_does_not_hold_rebalance_guard() {
        let reconciled = UsdcRebalance::Reconciled {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100.00)),
            reason: ReconcileReason::FundsMovedManually,
            failure_reason: None,
            initiated_at: Utc::now(),
            reconciled_at: Utc::now(),
        };
        assert!(
            !reconciled.holds_rebalance_guard(),
            "operator-reconciled terminal must clear the guard"
        );
    }

    #[test]
    fn reconciled_direction_reads_persisted_direction() {
        let reconciled = UsdcRebalance::Reconciled {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100.00)),
            reason: ReconcileReason::DepositCreditedOffline,
            failure_reason: None,
            initiated_at: Utc::now(),
            reconciled_at: Utc::now(),
        };
        assert_eq!(reconciled.direction(), RebalanceDirection::AlpacaToBase);
    }

    /// Reconciling from a `DepositFailed` state with `DepositCreditedOffline`
    /// reason must surface `reconcileReason = "deposit credited offline"` in
    /// the DTO -- an integration test covering the full
    /// command -> state -> to_dto pipeline for the `DepositCreditedOffline`
    /// variant, which is the operator's signal that the deposit settled
    /// out-of-band without needing a retry.
    #[tokio::test]
    async fn reconcile_deposit_credited_offline_surfaces_in_dto() {
        let history = deposit_failed_history();

        let events = TestHarness::<UsdcRebalance>::with()
            .given(history.clone())
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::DepositCreditedOffline,
            })
            .await
            .events();

        let reconciled_state = replay::<UsdcRebalance>([history, events].concat())
            .expect("event stream should replay")
            .expect("event stream should materialize aggregate state");

        let id = UsdcRebalanceId(uuid::Uuid::new_v4());
        let TransferOperation::UsdcBridge(bridge) = reconciled_state.to_dto(&id) else {
            panic!("expected UsdcBridge variant");
        };
        let serialized =
            serde_json::to_value(&bridge.status).expect("status serialization should succeed");
        assert_eq!(serialized["status"], serde_json::json!("reconciled"));
        assert_eq!(
            serialized["reconcileReason"],
            serde_json::json!("deposit credited offline"),
            "DepositCreditedOffline must surface the human-readable string in the DTO"
        );
        assert_eq!(
            serialized["failureReason"],
            serde_json::json!("deposit rejected"),
            "failure_reason must be recovered from the prior DepositFailed state"
        );
    }

    /// Simulates replaying a HISTORICAL `OperatorReconciled` event that was
    /// persisted before `failure_reason` was introduced (old events have no
    /// such field in their JSON). `apply()` must recover the failure reason
    /// from the prior `DepositFailed` state, so the replayed `Reconciled`
    /// state carries the correct context even without it in the event.
    #[tokio::test]
    async fn historical_operator_reconciled_event_recovers_failure_reason_from_prior_state() {
        // Build a history: DepositFailed then a legacy-style OperatorReconciled
        // (no failure_reason field -- matches what old persisted events look like).
        let mut history = deposit_failed_history();
        history.push(UsdcRebalanceEvent::OperatorReconciled {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100.00)),
            reason: ReconcileReason::FundsMovedManually,
            initiated_at: Utc::now(),
            reconciled_at: Utc::now(),
        });

        let state = replay::<UsdcRebalance>(history)
            .expect("event stream should replay")
            .expect("event stream should materialize aggregate state");

        let UsdcRebalance::Reconciled { failure_reason, .. } = state else {
            panic!("should be Reconciled, got {state:?}");
        };
        // "deposit rejected" is the reason set in deposit_failed_history().
        assert_eq!(
            failure_reason,
            Some("deposit rejected".to_string()),
            "historical replay must recover failure_reason from prior DepositFailed state"
        );
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_bridged() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_deposit_initiated() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_initiated() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                    "0x0000000000000000000000000000000000000000000000000000000000000001"
                )),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_already_reconciled() {
        let mut history = deposit_failed_history();
        history.push(UsdcRebalanceEvent::OperatorReconciled {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100.00)),
            reason: ReconcileReason::FundsMovedManually,
            initiated_at: Utc::now(),
            reconciled_at: Utc::now(),
        });

        let error = TestHarness::<UsdcRebalance>::with()
            .given(history)
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::DepositCreditedOffline,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_pre_burn_bridging_failed() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        // A pre-burn BridgingFailed (no burn tx, no CCTP nonce) carries no
        // burned funds: the failure already reconciles to source, so reconcile
        // must reject it rather than zero source inflight without crediting
        // available (which would lose the never-burned funds).
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: None,
                    cctp_nonce: None,
                    reason: "burn reverted before broadcast".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn reconcile_stuck_rebalance_rejected_from_alpaca_to_base_conversion_failed() {
        // AlpacaToBase ConversionFailed is the pre-withdrawal (pre-burn)
        // USD->USDC leg: no funds left the source, so reconcile must reject it.
        // It reconciles to source on its own via the normal failure path.
        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionFailed {
                    reason: "USD->USDC conversion rejected".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReconcileStuckRebalance {
                reason: ReconcileReason::FundsMovedManually,
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn test_deposit_failed_rejects_confirm_deposit() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(mint_tx),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositFailed {
                    deposit_ref: Some(TransferRef::OnchainTx(mint_tx)),
                    reason: "Deposit failed".to_string(),
                    failed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn test_deposit_confirmed_rejects_initiate() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(mint_tx),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(100.00)),
                withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::AlreadyInitiated)
        ));
    }

    #[tokio::test]
    async fn test_deposit_confirmed_rejects_confirm_deposit() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(mint_tx),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDeposit)
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { .. })
        ));
    }

    #[tokio::test]
    async fn test_initiate_conversion_from_uninitialized() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id: order_id.clone(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionInitiated {
            direction,
            amount,
            order_id: event_order_id,
            ..
        } = &events[0]
        else {
            panic!("Expected ConversionInitiated event");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc::new(float!(1000.00)));
        assert_eq!(*event_order_id, order_id);
    }

    #[tokio::test]
    async fn test_cannot_initiate_conversion_twice() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(500.00)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::AlreadyInitiated)
        ));
    }

    #[tokio::test]
    async fn test_confirm_conversion_from_converting_state() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let conversion = par_conversion(Usdc::new(float!(998)));

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmConversion { conversion })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::ConversionConfirmed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_conversion_before_initiating() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(Usdc::new(float!(998))),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::ConversionNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_conversion_twice() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    conversion: par_conversion(Usdc::new(float!(998))),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmConversion {
                conversion: par_conversion(Usdc::new(float!(998))),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::ConversionAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_fail_conversion_from_converting_state() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::FailConversion {
                reason: "Order rejected".to_string(),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionFailed { reason, .. } = &events[0] else {
            panic!("Expected ConversionFailed event");
        };
        assert_eq!(reason, "Order rejected");
    }

    #[tokio::test]
    async fn test_cannot_fail_conversion_before_initiating() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::FailConversion {
                reason: "Test failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::ConversionNotInitiated)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_completed_conversion() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    conversion: par_conversion(Usdc::new(float!(998))),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::FailConversion {
                reason: "Late failure".to_string(),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::ConversionAlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_after_conversion_complete() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let received_amount = Usdc::new(float!(998));

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    conversion: par_conversion(received_amount),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: received_amount,
                withdrawal: TransferRef::AlpacaId(transfer_id),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));
    }

    #[tokio::test]
    async fn test_initiate_with_mismatched_amount_from_conversion_complete_fails() {
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let received_amount = Usdc::new(float!(998));

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    conversion: par_conversion(received_amount),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(999.00)),
                withdrawal: TransferRef::AlpacaId(transfer_id),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                &error,
                LifecycleError::Apply(UsdcRebalanceError::InvalidCommand { command, state })
                    if command == "Initiate" && state.contains("amount mismatch")
            ),
            "Expected InvalidCommand error with amount mismatch, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_initiate_post_deposit_conversion_from_deposit_confirmed() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: order_id.clone(),
                // Must match deposit amount (amount_received from bridging, not original)
                amount: Usdc::new(float!(99.99)),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionInitiated {
            direction,
            amount,
            order_id: event_order_id,
            ..
        } = &events[0]
        else {
            panic!("Expected ConversionInitiated event");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc::new(float!(99.99)));
        assert_eq!(*event_order_id, order_id);
    }

    #[tokio::test]
    async fn test_cannot_initiate_post_deposit_conversion_for_alpaca_to_base() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::OnchainTx(mint_tx),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                amount: Usdc::new(float!(1000.00)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::WrongDirectionForPostDepositConversion)
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_post_deposit_conversion_before_deposit_confirmed() {
        let error = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                amount: Usdc::new(float!(1000.00)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::DepositNotConfirmed)
        ));
    }

    #[tokio::test]
    async fn test_initiate_post_deposit_conversion_rejects_mismatched_amount() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiatePostDepositConversion {
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                amount: Usdc::new(float!(500.00)),
            })
            .await
            .then_expect_error();

        // Deposit amount is amount_received (99.99), not original (1000)
        assert!(
            matches!(
                &error,
                LifecycleError::Apply(UsdcRebalanceError::ConversionAmountMismatch { expected, provided })
                    if *expected == Usdc::new(float!(99.99)) && *provided == Usdc::new(float!(500.00))
            ),
            "Expected ConversionAmountMismatch with expected=99.99 and provided=500, got: {error:?}"
        );
    }

    /// Covers the fixture-only `*At` siblings of the async transitions that
    /// take an explicit timestamp instead of `Utc::now()`: each must thread
    /// the caller-supplied timestamp through to the emitted event's field
    /// rather than silently falling back to the current time.
    #[tokio::test]
    async fn initiate_conversion_at_uses_supplied_timestamp() {
        let initiated_at = Utc::now() - chrono::Duration::hours(3);
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::InitiateConversionAt {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionInitiated {
            initiated_at: event_initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected ConversionInitiated event");
        };
        assert_eq!(*event_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn confirm_conversion_at_uses_supplied_timestamp() {
        let converted_at = Utc::now() - chrono::Duration::hours(2);
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let conversion = par_conversion(Usdc::new(float!(998)));

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmConversionAt {
                conversion,
                converted_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionConfirmed {
            converted_at: event_converted_at,
            ..
        } = &events[0]
        else {
            panic!("Expected ConversionConfirmed event");
        };
        assert_eq!(*event_converted_at, converted_at);
    }

    #[tokio::test]
    async fn initiate_post_deposit_conversion_at_uses_supplied_timestamp() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let initiated_at = Utc::now() - chrono::Duration::hours(1);

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    deposit_confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiatePostDepositConversionAt {
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                amount: Usdc::new(float!(99.99)),
                initiated_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionInitiated {
            initiated_at: event_initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected ConversionInitiated event");
        };
        assert_eq!(*event_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn begin_withdrawal_at_uses_supplied_timestamp() {
        let submitting_at = Utc::now() - chrono::Duration::hours(4);

        let events = TestHarness::<UsdcRebalance>::with()
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::BeginWithdrawalAt {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
                submitting_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalSubmitting {
            submitting_at: event_submitting_at,
            ..
        } = &events[0]
        else {
            panic!("Expected WithdrawalSubmitting event");
        };
        assert_eq!(*event_submitting_at, submitting_at);
    }

    #[tokio::test]
    async fn initiate_at_uses_supplied_timestamp() {
        let initiated_at = Utc::now() - chrono::Duration::hours(5);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::WithdrawalSubmitting {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                from_block: 42,
                submitting_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::InitiateAt {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::Initiated {
            initiated_at: event_initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected Initiated event");
        };
        assert_eq!(*event_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn confirm_withdrawal_at_uses_supplied_timestamp() {
        let confirmed_at = Utc::now() - chrono::Duration::hours(6);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(500.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmWithdrawalAt {
                withdrawal_tx: None,
                confirmed_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: event_confirmed_at,
            ..
        } = &events[0]
        else {
            panic!("Expected WithdrawalConfirmed event");
        };
        assert_eq!(*event_confirmed_at, confirmed_at);
    }

    #[tokio::test]
    async fn begin_bridging_at_uses_supplied_timestamp() {
        let submitting_at = Utc::now() - chrono::Duration::hours(7);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridgingAt {
                from_block: 100,
                burn_amount: None,
                submitting_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingSubmitting {
            submitting_at: event_submitting_at,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingSubmitting event");
        };
        assert_eq!(*event_submitting_at, submitting_at);
    }

    #[tokio::test]
    async fn initiate_bridging_at_uses_supplied_timestamp() {
        let burned_at = Utc::now() - chrono::Duration::hours(8);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridgingAt { burn_tx, burned_at })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated {
            burned_at: event_burned_at,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingInitiated event");
        };
        assert_eq!(*event_burned_at, burned_at);
    }

    #[tokio::test]
    async fn receive_attestation_at_uses_supplied_timestamp() {
        let attested_at = Utc::now() - chrono::Duration::hours(9);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestationAt {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: vec![0xDE, 0xAD],
                mint_scan_from_block: 100,
                attested_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgeAttestationReceived {
            attested_at: event_attested_at,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgeAttestationReceived event");
        };
        assert_eq!(*event_attested_at, attested_at);
    }

    #[tokio::test]
    async fn confirm_bridging_at_uses_supplied_timestamp() {
        let minted_at = Utc::now() - chrono::Duration::hours(10);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridgingAt {
                mint_tx,
                amount_received: Usdc::new(float!(499.50)),
                fee_collected: Usdc::new(float!(0.50)),
                minted_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::Bridged {
            minted_at: event_minted_at,
            ..
        } = &events[0]
        else {
            panic!("Expected Bridged event");
        };
        assert_eq!(*event_minted_at, minted_at);
    }

    #[tokio::test]
    async fn initiate_deposit_at_uses_supplied_timestamp() {
        let deposit_initiated_at = Utc::now() - chrono::Duration::hours(11);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(499.50)),
                    fee_collected: Usdc::new(float!(0.50)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::InitiateDepositAt {
                deposit: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                deposit_initiated_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositInitiated {
            deposit_initiated_at: event_deposit_initiated_at,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositInitiated event");
        };
        assert_eq!(*event_deposit_initiated_at, deposit_initiated_at);
    }

    #[tokio::test]
    async fn confirm_deposit_at_uses_supplied_timestamp() {
        let deposit_confirmed_at = Utc::now() - chrono::Duration::hours(12);
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(499.50)),
                    fee_collected: Usdc::new(float!(0.50)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmDepositAt {
                deposit_confirmed_at,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at: event_deposit_confirmed_at,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositConfirmed event");
        };
        assert_eq!(*event_deposit_confirmed_at, deposit_confirmed_at);
    }

    #[tokio::test]
    async fn test_full_base_to_alpaca_flow_with_post_deposit_conversion() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: burn_tx,
                    burned_at: Utc::now(),
                },
                UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: vec![0x01],
                    cctp_nonce: TEST_CCTP_NONCE,
                    message: None,
                    mint_scan_from_block: Some(100),
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc::new(float!(99.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                    minted_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositInitiated {
                    deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    deposit_initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::DepositConfirmed {
                    direction: RebalanceDirection::BaseToAlpaca,
                    deposit_confirmed_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmConversion {
                conversion: conversion(Usdc::new(float!(1000)), Usdc::new(float!(998))),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::ConversionConfirmed { .. }
        ));
    }

    #[test]
    fn to_dto_preserves_original_initiated_at_when_post_deposit_conversion_begins() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let original_initiated_at = Utc::now();
        let withdrawal_confirmed_at = original_initiated_at + chrono::Duration::seconds(30);
        let bridged_at = original_initiated_at + chrono::Duration::seconds(90);
        let deposit_initiated_at = original_initiated_at + chrono::Duration::seconds(120);
        let deposit_confirmed_at = original_initiated_at + chrono::Duration::seconds(150);
        let burn_tx =
            fixed_bytes!("0x000000000000000000000000000000000000000000000000000000000000000c");
        let mint_tx =
            fixed_bytes!("0x000000000000000000000000000000000000000000000000000000000000000d");

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                initiated_at: original_initiated_at,
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: withdrawal_confirmed_at,
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: withdrawal_confirmed_at,
            },
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01],
                cctp_nonce: TEST_CCTP_NONCE,
                message: None,
                mint_scan_from_block: Some(100),
                attested_at: withdrawal_confirmed_at + chrono::Duration::seconds(15),
            },
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash: mint_tx,
                amount_received: Usdc::new(float!(999.99)),
                fee_collected: Usdc::new(float!(0.01)),
                minted_at: bridged_at,
            },
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                deposit_initiated_at,
            },
            UsdcRebalanceEvent::DepositConfirmed {
                direction: RebalanceDirection::BaseToAlpaca,
                deposit_confirmed_at,
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(999.99)),
                order_id,
                initiated_at: original_initiated_at + chrono::Duration::seconds(180),
            },
        ])
        .expect("event stream should replay into post-deposit conversion state");
        let state = state.expect("event stream should materialize a UsdcRebalance state");

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(
            matches!(bridge.status, UsdcBridgeStatus::Converting),
            "expected dashboard to show post-deposit conversion as converting, got: {:?}",
            bridge.status
        );
        assert_eq!(
            bridge.started_at, original_initiated_at,
            "initiated_at should remain anchored to the original rebalance initiation time"
        );
        assert_eq!(bridge.updated_at, original_initiated_at);
    }

    /// After the fix that makes `Withdrawing.initiated_at` reflect the actual
    /// Alpaca withdrawal initiation time (from the `Initiated` event), the DTO's
    /// `started_at` and `updated_at` fields reflect when the withdrawal step began,
    /// not when the overall rebalance started. This is correct: the deadline alert
    /// is anchored to the withdrawal start, and the display is more precise.
    #[test]
    fn to_dto_uses_withdrawal_initiation_time_not_rebalance_start_when_withdrawing() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let original_initiated_at = Utc::now();
        let conversion_completed_at = original_initiated_at + chrono::Duration::seconds(30);
        let withdrawal_initiated_at = original_initiated_at + chrono::Duration::seconds(60);
        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: original_initiated_at,
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                conversion: conversion(Usdc::new(float!(1000)), Usdc::new(float!(999.99))),
                converted_at: conversion_completed_at,
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(999.99)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: withdrawal_initiated_at,
            },
        ])
        .expect("event stream should replay into withdrawing state");
        let state = state.expect("event stream should materialize a UsdcRebalance state");

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Withdrawing));
        // Withdrawing.initiated_at now reflects when the Alpaca withdrawal was initiated
        // (from the Initiated event), so the DTO's started_at and updated_at reflect
        // the withdrawal start time, not the overall rebalance start.
        assert_eq!(
            bridge.started_at, withdrawal_initiated_at,
            "started_at must reflect the withdrawal initiation time (from the Initiated event)"
        );
        assert_eq!(
            bridge.updated_at, withdrawal_initiated_at,
            "updated_at must reflect the withdrawal initiation time in Withdrawing state"
        );
    }

    #[test]
    fn conversion_confirmed_on_uninitialized_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![UsdcRebalanceEvent::ConversionConfirmed {
            direction: RebalanceDirection::BaseToAlpaca,
            conversion: conversion(Usdc::new(float!(1000)), Usdc::new(float!(998))),
            converted_at: Utc::now(),
        }])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::EventCantOriginate { .. }));
    }

    #[test]
    fn conversion_initiated_on_withdrawing_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: Utc::now(),
            },
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }

    #[test]
    fn to_dto_converting_maps_to_converting_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let state = UsdcRebalance::Converting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(500)),
            order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            initiated_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert_eq!(bridge.id, Id::new(id.to_string()));
        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::AlpacaToBase
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(500)));
        assert!(matches!(bridge.status, UsdcBridgeStatus::Converting));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, initiated_at);
    }

    #[test]
    fn to_dto_bridging_maps_to_bridging_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let burned_at = initiated_at + chrono::Duration::seconds(30);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000002");
        let state = UsdcRebalance::Bridging {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(2000)),
            burn_tx_hash: burn_tx,
            initiated_at,
            burned_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::BaseToAlpaca
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(2000)));
        assert!(matches!(bridge.status, UsdcBridgeStatus::Bridging));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, burned_at);
    }

    #[test]
    fn to_dto_awaiting_attestation_maps_to_bridging_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let timed_out_at = initiated_at + chrono::Duration::minutes(5);
        let state = UsdcRebalance::AwaitingAttestation {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(2000)),
            burn_tx_hash: BURN_TX,
            initiated_at,
            timed_out_at,
            retry_deadline_at: initiated_at + chrono::Duration::hours(24),
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Bridging));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, timed_out_at);
    }

    #[test]
    fn to_dto_deposit_confirmed_maps_to_completed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = initiated_at + chrono::Duration::seconds(120);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000003");
        let mint_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000004");
        let state = UsdcRebalance::DepositConfirmed {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: mint_tx,
            initiated_at,
            deposit_confirmed_at: confirmed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Completed { completed_at } if completed_at == confirmed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, confirmed_at);
    }

    #[test]
    fn to_dto_deposit_confirmed_base_to_alpaca_maps_to_converting() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = initiated_at + chrono::Duration::seconds(120);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000003");
        let mint_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000004");
        let state = UsdcRebalance::DepositConfirmed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(1000)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: mint_tx,
            initiated_at,
            deposit_confirmed_at: confirmed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Converting));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, confirmed_at);
    }

    #[test]
    fn to_dto_conversion_complete_alpaca_to_base_maps_to_converting() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let converted_at = initiated_at + chrono::Duration::seconds(30);
        let state = UsdcRebalance::ConversionComplete {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(500)),
            conversion: conversion(Usdc::new(float!(500)), Usdc::new(float!(499))),
            initiated_at,
            converted_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Converting));
        assert_eq!(bridge.amount, Usdc::new(float!(499)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, converted_at);
    }

    #[test]
    fn to_dto_conversion_complete_base_to_alpaca_maps_to_completed() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let converted_at = initiated_at + chrono::Duration::seconds(30);
        let state = UsdcRebalance::ConversionComplete {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(500)),
            conversion: conversion(Usdc::new(float!(500)), Usdc::new(float!(499))),
            initiated_at,
            converted_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Completed { completed_at } if completed_at == converted_at
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(499)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, converted_at);
    }

    #[test]
    fn to_dto_deposit_failed_maps_to_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000005");
        let mint_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000006");
        let state = UsdcRebalance::DepositFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(750)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: mint_tx,
            deposit_ref: None,
            reason: "deposit timeout".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: true
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_withdrawal_failed_maps_to_pre_burn_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let state = UsdcRebalance::WithdrawalFailed {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(750)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            reason: "withdrawal rejected".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // A withdrawal failure strands nothing on-chain, so the CLI rejects
        // `transfer reconcile --kind usdc` -- the discriminator must be false.
        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: false
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_bridging_failed_with_burn_tx_maps_to_post_burn_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000007");
        let state = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(750)),
            burn_tx_hash: Some(burn_tx),
            cctp_nonce: None,
            reason: "dropped from mempool".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // The burn landed on-chain, so funds left the source venue and the CLI
        // accepts `transfer reconcile --kind usdc` -- the discriminator is true.
        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: true
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_bridging_failed_without_burn_artifacts_maps_to_pre_burn_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let state = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(750)),
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "pre-burn failure".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // No burn tx and no CCTP nonce means nothing left the source venue, so
        // the CLI rejects reconcile -- the discriminator must be false.
        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: false
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_conversion_failed_base_to_alpaca_maps_to_post_burn_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let state = UsdcRebalance::ConversionFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(750)),
            order_id: ClientOrderId::from_uuid(Uuid::from_u128(13)),
            reason: "broker rejected".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // BaseToAlpaca conversion is the post-deposit USDC->USD leg: the burn
        // already happened, so the CLI accepts reconcile -- discriminator true.
        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: true
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_conversion_failed_alpaca_to_base_maps_to_pre_burn_failed_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = initiated_at + chrono::Duration::seconds(60);
        let state = UsdcRebalance::ConversionFailed {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(750)),
            order_id: ClientOrderId::from_uuid(Uuid::from_u128(14)),
            reason: "broker rejected".to_string(),
            initiated_at,
            failed_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // AlpacaToBase conversion is the pre-withdrawal USD->USDC leg: nothing
        // has burned yet, so the CLI rejects reconcile -- discriminator false.
        assert!(matches!(
            bridge.status,
            UsdcBridgeStatus::Failed {
                failed_at: fa,
                post_burn: false
            } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
    }

    #[test]
    fn to_dto_reconciled_carries_reconcile_reason_and_preserves_amount() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = "2026-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let reconciled_at = "2026-01-02T00:00:00Z".parse::<DateTime<Utc>>().unwrap();
        let state = UsdcRebalance::Reconciled {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(750)),
            reason: ReconcileReason::FundsMovedManually,
            failure_reason: Some("deposit timed out after max retries".to_string()),
            initiated_at,
            reconciled_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        // The funds genuinely left the source venue via CCTP burn, so the
        // projection must report the real transfer amount and its original
        // start time -- not a zero-value transfer beginning at reconciliation.
        assert_eq!(bridge.amount, Usdc::new(float!(750)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, reconciled_at);
        let serialized = serde_json::to_value(&bridge.status).expect("serialization failed");
        assert_eq!(serialized["status"], serde_json::json!("reconciled"));
        assert_eq!(
            serialized["reconciledAt"],
            serde_json::json!("2026-01-02T00:00:00Z")
        );
        assert_eq!(
            serialized["failureReason"],
            serde_json::json!("deposit timed out after max retries")
        );
        assert_eq!(
            serialized["reconcileReason"],
            serde_json::json!("funds moved manually")
        );
    }

    #[test]
    fn to_dto_withdrawing_maps_to_withdrawing_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let state = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(1000)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Withdrawing));
        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::AlpacaToBase
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(1000)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, initiated_at);
    }

    #[test]
    fn to_dto_withdrawal_complete_maps_to_withdrawing_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = initiated_at + chrono::Duration::seconds(45);
        let state = UsdcRebalance::WithdrawalComplete {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(800)),
            initiated_at,
            confirmed_at,
            withdrawal_tx: None,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Withdrawing));
        assert_eq!(bridge.amount, Usdc::new(float!(800)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, confirmed_at);
    }

    #[test]
    fn to_dto_attested_maps_to_bridging_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let attested_at = initiated_at + chrono::Duration::seconds(90);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000007");
        let state = UsdcRebalance::Attested {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(1500)),
            burn_tx_hash: burn_tx,
            cctp_nonce: TEST_CCTP_NONCE,
            attestation: vec![0xAA, 0xBB],
            message: None,
            mint_scan_from_block: Some(100),
            initiated_at,
            attested_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Bridging));
        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::BaseToAlpaca
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(1500)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, attested_at);
    }

    #[test]
    fn to_dto_bridged_maps_to_bridging_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let minted_at = initiated_at + chrono::Duration::seconds(120);
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000008");
        let mint_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000009");
        let state = UsdcRebalance::Bridged {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(2000)),
            amount_received: Usdc::new(float!(1998)),
            fee_collected: Usdc::new(float!(2)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: mint_tx,
            initiated_at,
            minted_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Bridging));
        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::AlpacaToBase
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(1998)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, minted_at);
    }

    #[test]
    fn to_dto_deposit_initiated_maps_to_depositing_status() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let initiated_at = Utc::now();
        let deposit_initiated_at = initiated_at + chrono::Duration::seconds(150);
        let burn_tx =
            fixed_bytes!("0x000000000000000000000000000000000000000000000000000000000000000a");
        let mint_tx =
            fixed_bytes!("0x000000000000000000000000000000000000000000000000000000000000000b");
        let state = UsdcRebalance::DepositInitiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(3000)),
            burn_tx_hash: burn_tx,
            mint_tx_hash: mint_tx,
            deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at,
            deposit_initiated_at,
        };

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Depositing));
        assert!(matches!(
            bridge.direction,
            st0x_dto::UsdcBridgeDirection::BaseToAlpaca
        ));
        assert_eq!(bridge.amount, Usdc::new(float!(3000)));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, deposit_initiated_at);
    }

    const BURN_TX: TxHash =
        fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000aa");
    const MINT_TX: TxHash =
        fixed_bytes!("0x00000000000000000000000000000000000000000000000000000000000000bb");

    #[test]
    fn holds_rebalance_guard_holds_for_in_progress_and_post_burn_failure() {
        use RebalanceDirection::{AlpacaToBase, BaseToAlpaca};
        use UsdcRebalance::*;

        let now = Utc::now();
        let amount = Usdc::new(float!(400.0));
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let withdrawal_ref = TransferRef::OnchainTx(BURN_TX);

        // In-progress states hold the guard, pre- and post-burn, both directions.
        assert!(
            Converting {
                direction: AlpacaToBase,
                amount,
                order_id,
                initiated_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            Withdrawing {
                direction: BaseToAlpaca,
                amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            WithdrawalComplete {
                direction: AlpacaToBase,
                amount,
                initiated_at: now,
                confirmed_at: now,
                withdrawal_tx: None,
            }
            .holds_rebalance_guard()
        );
        assert!(
            Bridging {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                initiated_at: now,
                burned_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            AwaitingAttestation {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                initiated_at: now,
                timed_out_at: now,
                retry_deadline_at: now + chrono::Duration::hours(24),
            }
            .holds_rebalance_guard()
        );
        assert!(
            Attested {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: BURN_TX,
                cctp_nonce: TEST_CCTP_NONCE,
                attestation: vec![],
                message: None,
                mint_scan_from_block: Some(100),
                initiated_at: now,
                attested_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            Bridged {
                direction: BaseToAlpaca,
                amount,
                amount_received: amount,
                fee_collected: Usdc::new(float!(0.0)),
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                initiated_at: now,
                minted_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            DepositInitiated {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                deposit_ref: withdrawal_ref.clone(),
                initiated_at: now,
                deposit_initiated_at: now,
            }
            .holds_rebalance_guard()
        );
        // Deposit failure is post-burn/post-mint -> holds the guard.
        assert!(
            DepositFailed {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                deposit_ref: Some(withdrawal_ref),
                reason: "deposit failed".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
        // Post-burn bridge failure keeps the guard; pre-burn failure clears it.
        assert!(
            BridgingFailed {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: Some(BURN_TX),
                cctp_nonce: None,
                reason: "post-burn".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            !BridgingFailed {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: None,
                cctp_nonce: None,
                reason: "pre-burn".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
    }

    #[test]
    fn holds_rebalance_guard_is_direction_aware_for_conversion_and_deposit_terminals() {
        use RebalanceDirection::{AlpacaToBase, BaseToAlpaca};
        use UsdcRebalance::*;

        let now = Utc::now();
        let amount = Usdc::new(float!(400.0));
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        // ConversionComplete: AlpacaToBase is the pre-withdrawal conversion (still
        // in progress); BaseToAlpaca is the post-deposit terminal success.
        assert!(
            ConversionComplete {
                direction: AlpacaToBase,
                amount,
                conversion: par_conversion(amount),
                initiated_at: now,
                converted_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            !ConversionComplete {
                direction: BaseToAlpaca,
                amount,
                conversion: par_conversion(amount),
                initiated_at: now,
                converted_at: now,
            }
            .holds_rebalance_guard()
        );

        // DepositConfirmed: AlpacaToBase is terminal; BaseToAlpaca still has the
        // post-deposit USDC->USD conversion ahead.
        assert!(
            !DepositConfirmed {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                initiated_at: now,
                deposit_confirmed_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            DepositConfirmed {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                initiated_at: now,
                deposit_confirmed_at: now,
            }
            .holds_rebalance_guard()
        );

        // ConversionFailed is direction-aware: AlpacaToBase's pre-withdrawal
        // conversion reconciles to source (clear); BaseToAlpaca's post-deposit
        // conversion is post-mint (hold).
        assert!(
            !ConversionFailed {
                direction: AlpacaToBase,
                amount,
                order_id: order_id.clone(),
                reason: "x".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            ConversionFailed {
                direction: BaseToAlpaca,
                amount,
                order_id,
                reason: "x".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
        // Withdrawal failure is always pre-burn -> clears.
        assert!(
            !WithdrawalFailed {
                direction: BaseToAlpaca,
                amount,
                withdrawal_ref: TransferRef::OnchainTx(BURN_TX),
                reason: "x".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .holds_rebalance_guard()
        );
    }

    /// The three manually-reconcilable guard-holding terminal states return
    /// `Some((direction, amount, failed_at))`: `DepositFailed` (any
    /// direction), `ConversionFailed(BaseToAlpaca)`, and
    /// `BridgingFailed(AlpacaToBase, burn_tx=Some)`.
    #[test]
    fn guard_recovery_tracking_data_returns_some_for_manually_reconcilable_states() {
        use RebalanceDirection::{AlpacaToBase, BaseToAlpaca};
        use UsdcRebalance::*;

        let now = Utc::now();
        let amount = Usdc::new(float!(400.0));
        let withdrawal_ref = TransferRef::OnchainTx(BURN_TX);

        let deposit_failed_bta = DepositFailed {
            direction: BaseToAlpaca,
            amount,
            burn_tx_hash: BURN_TX,
            mint_tx_hash: MINT_TX,
            deposit_ref: Some(withdrawal_ref.clone()),
            reason: "deposit rejected".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        let Some((direction, tracked_amount, last_progress_at)) =
            deposit_failed_bta.guard_recovery_tracking_data()
        else {
            panic!("expected Some for DepositFailed(BaseToAlpaca)");
        };
        assert_eq!(direction, BaseToAlpaca, "DepositFailed(BtA): direction");
        assert_eq!(tracked_amount, amount, "DepositFailed(BtA): amount");
        assert_eq!(last_progress_at, now, "DepositFailed(BtA): timestamp");

        let deposit_failed_atb = DepositFailed {
            direction: AlpacaToBase,
            amount,
            burn_tx_hash: BURN_TX,
            mint_tx_hash: MINT_TX,
            deposit_ref: Some(withdrawal_ref),
            reason: "deposit rejected".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        let Some((direction, tracked_amount, last_progress_at)) =
            deposit_failed_atb.guard_recovery_tracking_data()
        else {
            panic!("expected Some for DepositFailed(AlpacaToBase)");
        };
        assert_eq!(direction, AlpacaToBase, "DepositFailed(AtB): direction");
        assert_eq!(tracked_amount, amount, "DepositFailed(AtB): amount");
        assert_eq!(last_progress_at, now, "DepositFailed(AtB): timestamp");

        let conversion_failed_bta = ConversionFailed {
            direction: BaseToAlpaca,
            amount,
            order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
            reason: "order rejected".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        let Some((direction, tracked_amount, last_progress_at)) =
            conversion_failed_bta.guard_recovery_tracking_data()
        else {
            panic!("expected Some for ConversionFailed(BaseToAlpaca)");
        };
        assert_eq!(direction, BaseToAlpaca, "ConversionFailed(BtA): direction");
        assert_eq!(tracked_amount, amount, "ConversionFailed(BtA): amount");
        assert_eq!(last_progress_at, now, "ConversionFailed(BtA): timestamp");

        let bridging_failed_atb = BridgingFailed {
            direction: AlpacaToBase,
            amount,
            burn_tx_hash: Some(BURN_TX),
            cctp_nonce: None,
            reason: "unrecoverable".to_string(),
            initiated_at: now,
            failed_at: now,
        };
        let Some((direction, tracked_amount, last_progress_at)) =
            bridging_failed_atb.guard_recovery_tracking_data()
        else {
            panic!("expected Some for BridgingFailed(AlpacaToBase, burn_tx=Some)");
        };
        assert_eq!(direction, AlpacaToBase, "BridgingFailed(AtB): direction");
        assert_eq!(tracked_amount, amount, "BridgingFailed(AtB): amount");
        assert_eq!(last_progress_at, now, "BridgingFailed(AtB): timestamp");
    }

    /// All other states return `None`: guard-holding in-progress states
    /// (self-recover via reactor/apalis), the resumable
    /// `BridgingFailed(BaseToAlpaca, burn_tx=Some)` (re-armed on startup),
    /// pre-burn failures, and non-guard-holding terminals.
    // Each assert is a single trivial `None` check for a distinct enum variant.
    // Splitting into sub-functions would just move the line count elsewhere
    // without improving readability; the exhaustive flat sequence is the point.
    #[allow(clippy::too_many_lines)]
    #[test]
    fn guard_recovery_tracking_data_returns_none_for_self_recovering_and_terminal_states() {
        use RebalanceDirection::{AlpacaToBase, BaseToAlpaca};
        use UsdcRebalance::*;

        let now = Utc::now();
        let amount = Usdc::new(float!(400.0));
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let withdrawal_ref = TransferRef::OnchainTx(BURN_TX);

        // Guard-holding in-progress states: self-recover via reactor/apalis.
        assert_eq!(
            Converting {
                direction: AlpacaToBase,
                amount,
                order_id: order_id.clone(),
                initiated_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Converting(AlpacaToBase) must return None"
        );
        assert_eq!(
            Converting {
                direction: BaseToAlpaca,
                amount,
                order_id: order_id.clone(),
                initiated_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Converting(BaseToAlpaca) must return None"
        );
        assert_eq!(
            Withdrawing {
                direction: BaseToAlpaca,
                amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Withdrawing must return None"
        );
        assert_eq!(
            Bridging {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                initiated_at: now,
                burned_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Bridging must return None"
        );
        assert_eq!(
            Bridged {
                direction: BaseToAlpaca,
                amount,
                amount_received: amount,
                fee_collected: Usdc::new(float!(0.0)),
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                initiated_at: now,
                minted_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Bridged must return None"
        );
        assert_eq!(
            DepositInitiated {
                direction: AlpacaToBase,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                deposit_ref: withdrawal_ref.clone(),
                initiated_at: now,
                deposit_initiated_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "DepositInitiated must return None"
        );
        assert_eq!(
            DepositConfirmed {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                mint_tx_hash: MINT_TX,
                initiated_at: now,
                deposit_confirmed_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "DepositConfirmed(BaseToAlpaca) must return None"
        );
        // ConversionFailed(AlpacaToBase): pre-withdrawal leg, not guard-holding.
        assert_eq!(
            ConversionFailed {
                direction: AlpacaToBase,
                amount,
                order_id,
                reason: "x".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "ConversionFailed(AlpacaToBase) must return None (pre-burn)"
        );
        // BridgingFailed(BaseToAlpaca, burn_tx=Some): resumable, re-armed on
        // startup; seeding would wedge its DepositConfirmed path.
        assert_eq!(
            BridgingFailed {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: Some(BURN_TX),
                cctp_nonce: None,
                reason: "transient".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "BridgingFailed(BaseToAlpaca, burn_tx=Some) must return None (resumable)"
        );
        // BridgingFailed(burn_tx=None): pre-burn, not guard-holding.
        assert_eq!(
            BridgingFailed {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: None,
                cctp_nonce: None,
                reason: "pre-burn fail".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "BridgingFailed(burn_tx=None) must return None (pre-burn)"
        );
        // Guard-holding in-progress states not yet tested: these must all
        // return None because they self-recover via apalis; seeding them would
        // wedge their DepositConfirmed path.
        assert_eq!(
            ConversionComplete {
                direction: BaseToAlpaca,
                amount,
                conversion: par_conversion(amount),
                initiated_at: now,
                converted_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "ConversionComplete must return None (self-recovering)"
        );
        assert_eq!(
            WithdrawalSubmitting {
                direction: BaseToAlpaca,
                amount,
                from_block: 1,
                initiated_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "WithdrawalSubmitting must return None (self-recovering)"
        );
        assert_eq!(
            WithdrawalComplete {
                direction: BaseToAlpaca,
                amount,
                initiated_at: now,
                confirmed_at: now,
                withdrawal_tx: None,
            }
            .guard_recovery_tracking_data(),
            None,
            "WithdrawalComplete must return None (self-recovering)"
        );
        assert_eq!(
            BridgingSubmitting {
                direction: BaseToAlpaca,
                amount,
                from_block: 1,
                initiated_at: now,
                burn_amount: None,
                pending_burn_tx: None,
            }
            .guard_recovery_tracking_data(),
            None,
            "BridgingSubmitting must return None (self-recovering)"
        );
        assert_eq!(
            AwaitingAttestation {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                initiated_at: now,
                timed_out_at: now,
                retry_deadline_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "AwaitingAttestation must return None (self-recovering)"
        );
        assert_eq!(
            Attested {
                direction: BaseToAlpaca,
                amount,
                burn_tx_hash: BURN_TX,
                cctp_nonce: alloy::primitives::B256::ZERO,
                attestation: vec![],
                message: None,
                mint_scan_from_block: None,
                initiated_at: now,
                attested_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Attested must return None (self-recovering)"
        );
        // Non-guard-holding terminal states.
        assert_eq!(
            Reconciled {
                direction: BaseToAlpaca,
                amount,
                reason: ReconcileReason::FundsMovedManually,
                failure_reason: None,
                initiated_at: now,
                reconciled_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "Reconciled must return None"
        );
        assert_eq!(
            WithdrawalFailed {
                direction: BaseToAlpaca,
                amount,
                withdrawal_ref,
                reason: "x".to_string(),
                initiated_at: now,
                failed_at: now,
            }
            .guard_recovery_tracking_data(),
            None,
            "WithdrawalFailed must return None"
        );
    }

    /// Seeds a fresh aggregate by sending `commands` in order, returning its id.
    async fn seed_through(
        store: &Store<UsdcRebalance>,
        commands: Vec<UsdcRebalanceCommand>,
    ) -> UsdcRebalanceId {
        let id = UsdcRebalanceId(Uuid::new_v4());
        for command in commands {
            store.send(&id, command).await.unwrap();
        }
        id
    }

    #[tokio::test]
    async fn interrupted_usdc_rebalance_ids_excludes_clearable_terminals() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone());
        let amount = Usdc::new(float!(400.0));

        // Clearable terminal (withdrawal failed, pre-burn) -- must be excluded,
        // so its id is intentionally unbound after seeding.
        let _withdrawal_failed = seed_through(
            &store,
            vec![
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "x".to_string(),
                },
            ],
        )
        .await;

        // Post-burn bridge failure -- must be included.
        let bridge_failed = seed_through(
            &store,
            vec![
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
                UsdcRebalanceCommand::InitiateBridging { burn_tx: BURN_TX },
                UsdcRebalanceCommand::FailBridging {
                    reason: "x".to_string(),
                },
            ],
        )
        .await;

        // Awaiting attestation after a post-burn timeout -- must be included.
        let awaiting_attestation = seed_through(
            &store,
            vec![
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
                UsdcRebalanceCommand::InitiateBridging { burn_tx: BURN_TX },
                UsdcRebalanceCommand::TimeoutAttestation {
                    retry_deadline_at: Utc::now() + chrono::Duration::hours(24),
                },
            ],
        )
        .await;

        // Recovered post-burn bridge (latest event BridgingCompletionRecovered,
        // state Bridged) -- mid-flight after un-fail, holds the guard, must be
        // included so a crash before the deposit leg reasserts usdc_in_progress.
        let recovered = seed_through(
            &store,
            vec![
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
                UsdcRebalanceCommand::InitiateBridging { burn_tx: BURN_TX },
                UsdcRebalanceCommand::FailBridging {
                    reason: "transient receipt error".to_string(),
                },
                UsdcRebalanceCommand::RecoverBridging {
                    mint_tx: fixed_bytes!(
                        "0x2222222222222222222222222222222222222222222222222222222222222222"
                    ),
                    amount_received: Usdc::new(float!(399.99)),
                    fee_collected: Usdc::new(float!(0.01)),
                },
            ],
        )
        .await;

        // Pre-burn mid-flight whose LATEST event is PendingBurnRecorded (the burn
        // hash was recorded before its receipt) -- must be included, else the guard
        // latches with no driving job (the double-burn-safe redrive never runs).
        let pending_burn_recorded = seed_through(
            &store,
            vec![
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
                UsdcRebalanceCommand::ConfirmWithdrawal {
                    withdrawal_tx: None,
                },
                UsdcRebalanceCommand::BeginBridging {
                    from_block: 1,
                    burn_amount: None,
                },
                UsdcRebalanceCommand::RecordPendingBurn { burn_tx: BURN_TX },
            ],
        )
        .await;

        // In-progress (mid-withdrawal) -- must be included.
        let withdrawing = seed_through(
            &store,
            vec![UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount,
                withdrawal: TransferRef::OnchainTx(BURN_TX),
            }],
        )
        .await;

        let interrupted = interrupted_usdc_rebalance_ids(&pool).await.unwrap();
        let got: HashSet<UsdcRebalanceId> = interrupted.ids.into_iter().collect();

        assert_eq!(
            got,
            HashSet::from([
                bridge_failed,
                awaiting_attestation,
                recovered,
                pending_burn_recorded,
                withdrawing
            ])
        );
        assert!(
            interrupted.unparseable.is_empty(),
            "well-formed UUID aggregate_ids must not be reported as unparseable"
        );
    }

    // --- T1-T5: crash-safe AlpacaToBase machinery ---

    /// T1: ConfirmWithdrawal with Some(tx) emits WithdrawalConfirmed carrying
    /// the tx hash, then transitions WithdrawalComplete to carry it too.
    #[tokio::test]
    async fn confirm_withdrawal_with_tx_hash_threads_it_through_to_withdrawal_complete() {
        let tx_hash =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: Some(tx_hash),
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalConfirmed { withdrawal_tx, .. } = &events[0] else {
            panic!("Expected WithdrawalConfirmed, got {:?}", events[0]);
        };
        assert_eq!(*withdrawal_tx, Some(tx_hash));

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            },
            events[0].clone(),
        ])
        .unwrap();

        let Some(UsdcRebalance::WithdrawalComplete { withdrawal_tx, .. }) = state else {
            panic!("Expected WithdrawalComplete state, got: {state:?}");
        };
        assert_eq!(withdrawal_tx, Some(tx_hash));
    }

    /// T2: ConfirmWithdrawal with None works identically for the None case.
    #[tokio::test]
    async fn confirm_withdrawal_with_none_produces_withdrawal_complete_with_none_tx() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal {
                withdrawal_tx: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalConfirmed { withdrawal_tx, .. } = &events[0] else {
            panic!("Expected WithdrawalConfirmed, got {:?}", events[0]);
        };
        assert_eq!(*withdrawal_tx, None);

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc::new(float!(100.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            },
            events[0].clone(),
        ])
        .unwrap();

        let Some(UsdcRebalance::WithdrawalComplete { withdrawal_tx, .. }) = state else {
            panic!("Expected WithdrawalComplete state, got: {state:?}");
        };
        assert_eq!(withdrawal_tx, None);
    }

    /// T3: Backward compatibility -- replaying a persisted WithdrawalConfirmed
    /// that lacks the `withdrawal_tx` field (simulated via JSON without the field)
    /// deserializes via `#[serde(default)]` to `None`, producing WithdrawalComplete
    /// with `withdrawal_tx: None`.
    #[test]
    fn withdrawal_confirmed_without_withdrawal_tx_field_deserializes_to_none() {
        let old_event = json!({
            "WithdrawalConfirmed": {
                "confirmed_at": "2026-01-01T00:00:00Z"
            }
        });

        let event: UsdcRebalanceEvent =
            from_value(old_event).expect("old WithdrawalConfirmed must still deserialize");

        let UsdcRebalanceEvent::WithdrawalConfirmed { withdrawal_tx, .. } = event else {
            panic!("Expected WithdrawalConfirmed");
        };
        assert_eq!(withdrawal_tx, None, "missing field must default to None");
    }

    /// T3b: State-level mirror of T3 -- a `WithdrawalComplete` snapshot persisted
    /// before `withdrawal_tx` existed must still load, defaulting the field to
    /// `None` via `#[serde(default)]`. Guards the restart/resume path: dropping
    /// the default or adding `deny_unknown_fields` would strand in-flight
    /// transfers held in a pre-field `WithdrawalComplete` snapshot.
    #[test]
    fn withdrawal_complete_snapshot_without_withdrawal_tx_deserializes_to_none() {
        let tx_hash =
            fixed_bytes!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let mut snapshot = to_value(UsdcRebalance::WithdrawalComplete {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100.00)),
            initiated_at: Utc::now(),
            confirmed_at: Utc::now(),
            withdrawal_tx: Some(tx_hash),
        })
        .expect("WithdrawalComplete state serializes");

        // Simulate a snapshot persisted before the `withdrawal_tx` field existed.
        snapshot
            .get_mut("WithdrawalComplete")
            .and_then(|value| value.as_object_mut())
            .expect("externally-tagged WithdrawalComplete object")
            .remove("withdrawal_tx");

        let state = from_value::<UsdcRebalance>(snapshot)
            .expect("pre-field WithdrawalComplete snapshot must still load");

        let UsdcRebalance::WithdrawalComplete { withdrawal_tx, .. } = state else {
            panic!("expected WithdrawalComplete, got {state:?}");
        };

        assert_eq!(withdrawal_tx, None);
    }

    /// A `BridgingSubmitting` event persisted before `burn_amount` existed must
    /// still deserialize, defaulting the field to `None` via `#[serde(default)]`.
    /// Dropping the default would strand legacy in-flight bridging transfers.
    #[test]
    fn bridging_submitting_event_without_burn_amount_deserializes_to_none() {
        let old_event = json!({
            "BridgingSubmitting": {
                "from_block": 42,
                "submitting_at": "2026-01-01T00:00:00Z"
            }
        });

        let event: UsdcRebalanceEvent =
            from_value(old_event).expect("old BridgingSubmitting must still deserialize");

        let UsdcRebalanceEvent::BridgingSubmitting { burn_amount, .. } = event else {
            panic!("Expected BridgingSubmitting");
        };
        assert_eq!(burn_amount, None, "missing field must default to None");
    }

    /// State-level mirror: a `BridgingSubmitting` snapshot persisted before
    /// `burn_amount` existed must still load, defaulting the field to `None`.
    #[test]
    fn bridging_submitting_snapshot_without_burn_amount_deserializes_to_none() {
        let mut snapshot = to_value(UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100.00)),
            from_block: 42,
            initiated_at: Utc::now(),
            burn_amount: Some(Usdc::new(float!(99.99))),
            pending_burn_tx: None,
        })
        .expect("BridgingSubmitting state serializes");

        // Simulate a snapshot persisted before the `burn_amount` field existed.
        snapshot
            .get_mut("BridgingSubmitting")
            .and_then(|value| value.as_object_mut())
            .expect("externally-tagged BridgingSubmitting object")
            .remove("burn_amount");

        let state = from_value::<UsdcRebalance>(snapshot)
            .expect("pre-field BridgingSubmitting snapshot must still load");

        let UsdcRebalance::BridgingSubmitting { burn_amount, .. } = state else {
            panic!("expected BridgingSubmitting, got {state:?}");
        };

        assert_eq!(burn_amount, None);
    }

    /// T4: BeginBridging from WithdrawalComplete (AlpacaToBase direction) emits
    /// BridgingSubmitting and transitions to BridgingSubmitting state carrying
    /// direction: AlpacaToBase.
    #[tokio::test]
    async fn begin_bridging_from_withdrawal_complete_alpaca_to_base_transitions_correctly() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(200.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging {
                from_block: 42,
                burn_amount: None,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingSubmitting { from_block, .. } = &events[0] else {
            panic!("Expected BridgingSubmitting event, got {:?}", events[0]);
        };
        assert_eq!(*from_block, 42);

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(200.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            events[0].clone(),
        ])
        .unwrap();

        let Some(UsdcRebalance::BridgingSubmitting {
            direction,
            from_block,
            ..
        }) = state
        else {
            panic!("Expected BridgingSubmitting state, got: {state:?}");
        };
        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(from_block, 42);
    }

    /// T5: BridgingSubmitting (AlpacaToBase direction): InitiateBridging transitions
    /// to Bridging. Verifies InitiateBridging is direction-agnostic.
    #[tokio::test]
    async fn initiate_bridging_from_bridging_submitting_alpaca_to_base_transitions_to_bridging() {
        let burn_tx =
            fixed_bytes!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with()
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(200.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                    withdrawal_tx: None,
                },
                UsdcRebalanceEvent::BridgingSubmitting {
                    from_block: 42,
                    submitting_at: Utc::now(),
                    burn_amount: None,
                },
            ])
            .when(UsdcRebalanceCommand::InitiateBridging { burn_tx })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated { burn_tx_hash, .. } = &events[0] else {
            panic!("Expected BridgingInitiated event, got {:?}", events[0]);
        };
        assert_eq!(*burn_tx_hash, burn_tx);

        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(200.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::WithdrawalConfirmed {
                confirmed_at: Utc::now(),
                withdrawal_tx: None,
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 42,
                submitting_at: Utc::now(),
                burn_amount: None,
            },
            events[0].clone(),
        ])
        .unwrap();

        let Some(UsdcRebalance::Bridging {
            direction,
            burn_tx_hash,
            ..
        }) = state
        else {
            panic!("Expected Bridging state, got: {state:?}");
        };
        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(burn_tx_hash, burn_tx);
    }

    // --- is_resumable_mid_flight tests ---

    fn bridging_submitting_base_to_alpaca() -> UsdcRebalance {
        UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            from_block: 1,
            initiated_at: Utc::now(),
            burn_amount: None,
            pending_burn_tx: None,
        }
    }

    fn bridging_submitting_alpaca_to_base() -> UsdcRebalance {
        UsdcRebalance::BridgingSubmitting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100)),
            from_block: 1,
            initiated_at: Utc::now(),
            burn_amount: None,
            pending_burn_tx: None,
        }
    }

    fn withdrawal_submitting_base_to_alpaca() -> UsdcRebalance {
        UsdcRebalance::WithdrawalSubmitting {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            from_block: 1,
            initiated_at: Utc::now(),
        }
    }

    fn withdrawal_submitting_alpaca_to_base() -> UsdcRebalance {
        UsdcRebalance::WithdrawalSubmitting {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100)),
            from_block: 1,
            initiated_at: Utc::now(),
        }
    }

    #[test]
    fn is_resumable_mid_flight_data_some_for_bridging_submitting_base_to_alpaca() {
        let amount = Usdc::new(float!(100));
        let result = bridging_submitting_base_to_alpaca().is_resumable_mid_flight_data();
        assert_eq!(
            result,
            Some((RebalanceDirection::BaseToAlpaca, amount)),
            "BridgingSubmitting BaseToAlpaca must be resumable"
        );
    }

    #[test]
    fn is_resumable_mid_flight_data_some_for_bridging_submitting_alpaca_to_base() {
        let amount = Usdc::new(float!(100));
        let result = bridging_submitting_alpaca_to_base().is_resumable_mid_flight_data();
        assert_eq!(
            result,
            Some((RebalanceDirection::AlpacaToBase, amount)),
            "BridgingSubmitting AlpacaToBase must be resumable"
        );
    }

    #[test]
    fn is_resumable_mid_flight_data_some_for_withdrawal_submitting_base_to_alpaca() {
        let amount = Usdc::new(float!(100));
        let result = withdrawal_submitting_base_to_alpaca().is_resumable_mid_flight_data();
        assert_eq!(
            result,
            Some((RebalanceDirection::BaseToAlpaca, amount)),
            "WithdrawalSubmitting BaseToAlpaca must be resumable (resume_base_to_alpaca handles it)"
        );
    }

    #[test]
    fn is_resumable_mid_flight_data_none_for_withdrawal_submitting_alpaca_to_base() {
        let result = withdrawal_submitting_alpaca_to_base().is_resumable_mid_flight_data();
        assert_eq!(
            result, None,
            "WithdrawalSubmitting AlpacaToBase must NOT be resumable \
             (resume_alpaca_to_base returns ResumeDirectionMismatch)"
        );
    }

    /// `Withdrawing{AlpacaToBase}` is resumable mid-flight: the AlpacaTransferId
    /// is durably recorded and `resume_alpaca_to_base` re-polls the same transfer.
    /// The returned `(direction, amount)` tuple must match exactly.
    #[test]
    fn withdrawing_alpaca_to_base_is_resumable_mid_flight_data() {
        let amount = Usdc::new(float!(100));
        let state = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        };

        assert_eq!(
            state.is_resumable_mid_flight_data(),
            Some((RebalanceDirection::AlpacaToBase, amount)),
            "Withdrawing{{AlpacaToBase}} must be resumable mid-flight with the correct amount"
        );
    }

    /// A malformed persisted `Withdrawing{AlpacaToBase}` carrying an on-chain
    /// withdrawal ref has no Alpaca transfer ID to re-poll, so startup recovery
    /// must not re-arm it as a mid-flight Alpaca withdrawal.
    #[test]
    fn withdrawing_alpaca_to_base_onchain_ref_is_not_resumable_mid_flight_data() {
        let state = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc::new(float!(100)),
            withdrawal_ref: TransferRef::OnchainTx(alloy::primitives::TxHash::ZERO),
            initiated_at: Utc::now(),
        };

        assert_eq!(
            state.is_resumable_mid_flight_data(),
            None,
            "Withdrawing{{AlpacaToBase}} with an on-chain withdrawal ref must not be \
             auto-rearmed without an Alpaca transfer id"
        );
    }

    /// `WithdrawalComplete{AlpacaToBase}` is resumable mid-flight: Alpaca has
    /// already moved the source funds, and `resume_alpaca_to_base` scans for an
    /// existing burn before submitting one.
    #[test]
    fn withdrawal_complete_alpaca_to_base_is_resumable_mid_flight_data() {
        let amount = Usdc::new(float!(100));
        let state = UsdcRebalance::WithdrawalComplete {
            direction: RebalanceDirection::AlpacaToBase,
            amount,
            initiated_at: Utc::now(),
            confirmed_at: Utc::now(),
            withdrawal_tx: None,
        };

        assert_eq!(
            state.is_resumable_mid_flight_data(),
            Some((RebalanceDirection::AlpacaToBase, amount)),
            "WithdrawalComplete{{AlpacaToBase}} must be resumable mid-flight with the correct amount"
        );
    }

    /// `Withdrawing{BaseToAlpaca}` is NOT resumable mid-flight. BaseToAlpaca
    /// reaches Withdrawing via an on-chain tx (handled by the post-burn path,
    /// not the mid-flight re-arm path).
    #[test]
    fn withdrawing_base_to_alpaca_is_not_resumable_mid_flight_data() {
        let state = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            withdrawal_ref: TransferRef::OnchainTx(alloy::primitives::TxHash::ZERO),
            initiated_at: Utc::now(),
        };

        assert_eq!(
            state.is_resumable_mid_flight_data(),
            None,
            "Withdrawing{{BaseToAlpaca}} must not be resumable mid-flight"
        );
    }

    /// `WithdrawalComplete{BaseToAlpaca}` is NOT resumable mid-flight. That
    /// direction's post-withdrawal leg is handled by the post-burn recovery path.
    #[test]
    fn withdrawal_complete_base_to_alpaca_is_not_resumable_mid_flight_data() {
        let state = UsdcRebalance::WithdrawalComplete {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            initiated_at: Utc::now(),
            confirmed_at: Utc::now(),
            withdrawal_tx: Some(alloy::primitives::TxHash::ZERO),
        };

        assert_eq!(
            state.is_resumable_mid_flight_data(),
            None,
            "WithdrawalComplete{{BaseToAlpaca}} must not be resumable mid-flight"
        );
    }

    /// `WithdrawalSubmitting{AlpacaToBase}` STILL returns None after this change
    /// -- regression pin. It must not accidentally become resumable.
    #[test]
    fn withdrawal_submitting_alpaca_to_base_remains_not_resumable_mid_flight() {
        assert_eq!(
            withdrawal_submitting_alpaca_to_base().is_resumable_mid_flight_data(),
            None,
            "WithdrawalSubmitting{{AlpacaToBase}} must still not be resumable mid-flight \
             (transfer ID not yet persisted in that state)"
        );
    }

    /// Regression: `Withdrawing.initiated_at` must come from the `Initiated`
    /// event's timestamp (set at withdrawal initiation time in
    /// `transition_initiate_withdrawal`), NOT from the prior state's `initiated_at`
    /// which tracks the overall rebalance start. Before this fix both evolve arms
    /// for `AlpacaToBase` copied the prior state's value, so a rebalance with a
    /// long conversion phase would have an already-elapsed 4-hour operator-alert
    /// deadline at the first inconclusive withdrawal poll after restart.
    #[tokio::test]
    async fn withdrawing_alpaca_to_base_initiated_at_reflects_withdrawal_time_not_rebalance_start()
    {
        // Represent the rebalance start as 5 hours ago -- well past the 4-hour
        // operator-alert deadline. After the fix, Withdrawing.initiated_at must be
        // the withdrawal initiation time (recent), not the old rebalance start.
        let old_rebalance_start = Utc::now() - chrono::Duration::hours(5);
        let amount = Usdc::new(float!(100));
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let prior_events = vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                order_id: ClientOrderId::from_uuid(Uuid::new_v4()),
                initiated_at: old_rebalance_start,
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                conversion: par_conversion(amount),
                converted_at: old_rebalance_start + chrono::Duration::minutes(10),
            },
        ];

        // Apply the Initiate command; transition_initiate_withdrawal emits
        // Initiated { initiated_at: Utc::now() } at the withdrawal moment.
        let before_initiate = Utc::now();
        let new_events = TestHarness::<UsdcRebalance>::with()
            .given(prior_events.clone())
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount,
                withdrawal: TransferRef::AlpacaId(transfer_id),
            })
            .await
            .events();
        let after_initiate = Utc::now();

        assert_eq!(
            new_events.len(),
            1,
            "Initiate must emit exactly one Initiated event"
        );

        // Extract the event's initiated_at before consuming new_events in the replay.
        let event_initiated_at = match &new_events[0] {
            UsdcRebalanceEvent::Initiated { initiated_at, .. } => *initiated_at,
            other => panic!("Expected Initiated event, got {other:?}"),
        };

        // Replay all events to get the Withdrawing state.
        let all_events: Vec<_> = prior_events.into_iter().chain(new_events).collect();
        let state = replay::<UsdcRebalance>(all_events)
            .expect("events must replay cleanly into Withdrawing")
            .expect("aggregate must have state after events");

        let UsdcRebalance::Withdrawing {
            initiated_at: withdrawing_initiated_at,
            ..
        } = state
        else {
            panic!("Expected Withdrawing state, got: {state:?}");
        };

        // The Withdrawing state's initiated_at must equal the event's timestamp.
        assert_eq!(
            withdrawing_initiated_at, event_initiated_at,
            "Withdrawing.initiated_at must match the Initiated event's initiated_at \
             (withdrawal initiation time), not the prior ConversionComplete.initiated_at"
        );

        // The withdrawal initiation time must be recent (near the Initiate call),
        // not the old rebalance start (5 hours ago).
        assert!(
            withdrawing_initiated_at >= before_initiate
                && withdrawing_initiated_at <= after_initiate,
            "Withdrawing.initiated_at must be the withdrawal initiation time (recent), \
             not the old rebalance start {old_rebalance_start:?}; got {withdrawing_initiated_at:?}"
        );
    }

    #[test]
    fn is_resumable_mid_flight_data_none_for_post_burn_bridging() {
        let state = UsdcRebalance::Bridging {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            burn_tx_hash: alloy::primitives::TxHash::ZERO,
            initiated_at: Utc::now(),
            burned_at: Utc::now(),
        };
        assert_eq!(
            state.is_resumable_mid_flight_data(),
            None,
            "Bridging (post-burn) must not be mid-flight resumable"
        );
    }

    #[test]
    fn is_resumable_mid_flight_data_none_for_terminal_bridging_failed() {
        let state = UsdcRebalance::BridgingFailed {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc::new(float!(100)),
            burn_tx_hash: None,
            cctp_nonce: None,
            reason: "transient".to_string(),
            initiated_at: Utc::now(),
            failed_at: Utc::now(),
        };
        assert_eq!(
            state.is_resumable_mid_flight_data(),
            None,
            "BridgingFailed (terminal) must not be mid-flight resumable"
        );
    }
}
