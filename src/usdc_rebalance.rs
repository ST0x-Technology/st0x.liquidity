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
//! - BridgingFailed
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
//! Terminal states reject all commands to prevent invalid state transitions.
//!
//! [`AlpacaWalletService`]: crate::alpaca_wallet::AlpacaWalletService

use alloy::primitives::{B256, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use tracing::warn;
use uuid::Uuid;

use st0x_dto::{TransferOperation, UsdcBridgeOperation, UsdcBridgeStatus};
use st0x_event_sorcery::{DomainEvent, EventSourced, Nil};
use st0x_execution::ClientOrderId;
use st0x_finance::{Id, Usdc};

use crate::alpaca_wallet::AlpacaTransferId;

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum RebalanceDirection {
    AlpacaToBase,
    BaseToAlpaca,
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
    /// Bridging has not been initiated yet
    #[error("Bridging has not been initiated")]
    BridgingNotInitiated,
    /// Attestation has not been received yet
    #[error("Attestation has not been received")]
    AttestationNotReceived,
    /// Bridging has already completed or failed
    #[error("Bridging has already completed")]
    BridgingAlreadyCompleted,
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
    /// Confirm successful conversion. Valid only from `Converting` state.
    /// Contains filled_amount for accurate inventory tracking.
    ConfirmConversion {
        /// Actual USDC amount from the conversion.
        /// - AlpacaToBase (USD->USDC): USDC received
        /// - BaseToAlpaca (USDC->USD): USDC sold
        filled_amount: Usdc,
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
    /// Start a new rebalancing operation by recording the submitted withdrawal
    /// transaction. Valid only from `WithdrawalSubmitting` state.
    Initiate {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
    },
    /// Confirm successful withdrawal from source. Valid only from `Withdrawing` state.
    ConfirmWithdrawal,
    /// Record the intent to burn for CCTP bridging BEFORE the on-chain call.
    /// Captures the chain head (`from_block`) for crash recovery, mirroring
    /// [`BeginWithdrawal`]. Valid only from `WithdrawalComplete` state.
    BeginBridging { from_block: u64 },
    /// Record the CCTP burn transaction. Valid only from `BridgingSubmitting` state.
    InitiateBridging { burn_tx: TxHash },
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
    /// Start deposit to destination. Valid only from `Bridged` state.
    InitiateDeposit { deposit: TransferRef },
    /// Confirm successful deposit. Valid only from `DepositInitiated` state.
    ConfirmDeposit,
    /// Record withdrawal failure. Valid only from `Withdrawing` state.
    FailWithdrawal { reason: String },
    /// Record bridging failure. Valid from `Bridging` or `Attested` states.
    FailBridging { reason: String },
    /// Record deposit failure. Valid only from `DepositInitiated` state.
    FailDeposit { reason: String },
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
    /// Contains filled_amount for accurate inventory tracking (may differ from requested due to slippage).
    ConversionConfirmed {
        direction: RebalanceDirection,
        /// Actual USDC amount from the conversion.
        /// - AlpacaToBase (USD->USDC): USDC received
        /// - BaseToAlpaca (USDC->USD): USDC sold
        filled_amount: Usdc,
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
    WithdrawalConfirmed { confirmed_at: DateTime<Utc> },
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
    },
    /// CCTP burn transaction submitted. Records burn hash for attestation lookup.
    BridgingInitiated {
        burn_tx_hash: TxHash,
        burned_at: DateTime<Utc>,
    },
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
            Self::BridgeAttestationReceived { .. } => {
                "UsdcRebalanceEvent::BridgeAttestationReceived"
            }
            Self::AttestationTimedOut { .. } => "UsdcRebalanceEvent::AttestationTimedOut",
            Self::Bridged { .. } => "UsdcRebalanceEvent::Bridged",
            Self::BridgingFailed { .. } => "UsdcRebalanceEvent::BridgingFailed",
            Self::DepositInitiated { .. } => "UsdcRebalanceEvent::DepositInitiated",
            Self::DepositConfirmed { .. } => "UsdcRebalanceEvent::DepositConfirmed",
            Self::DepositFailed { .. } => "UsdcRebalanceEvent::DepositFailed",
        }
        .to_string()
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
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
        /// Actual USDC amount from the conversion (may differ due to slippage)
        filled_amount: Usdc,
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
}

impl UsdcRebalance {
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
                filled_amount,
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
                    *filled_amount,
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
    /// re-opening the re-burn window. See ADR 2.
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
            // re-burn cannot fire against funds CCTP already burned. The
            // `FailBridging` command only emits `burn_tx_hash: Some` from the
            // post-burn `Bridging`/`Attested` states; a pre-burn failure (from
            // `WithdrawalComplete`) carries `None` and reconciles to source.
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
            // Withdrawal failure is always pre-burn: reconcile to source and clear.
            Self::WithdrawalFailed { .. } => false,
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
            Self::Bridging {
                direction, amount, ..
            }
            | Self::AwaitingAttestation {
                direction, amount, ..
            }
            | Self::Attested {
                direction, amount, ..
            } => Some((direction.clone(), *amount)),
            _ => None,
        }
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
               'UsdcRebalanceEvent::BridgingInitiated', \
               'UsdcRebalanceEvent::BridgeAttestationReceived', \
               'UsdcRebalanceEvent::AttestationTimedOut', \
               'UsdcRebalanceEvent::Bridged', \
               'UsdcRebalanceEvent::BridgingFailed', \
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

#[async_trait]
impl EventSourced for UsdcRebalance {
    type Id = UsdcRebalanceId;
    type Event = UsdcRebalanceEvent;
    type Command = UsdcRebalanceCommand;
    type Error = UsdcRebalanceError;
    type Services = ();
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
    const SCHEMA_VERSION: u64 = 4;

    fn originate(event: &Self::Event) -> Option<Self> {
        use UsdcRebalanceEvent::*;
        match event {
            ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at,
            } => Some(Self::Converting {
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
                amount: *amount,
                order_id: order_id.clone(),
                initiated_at: *initiated_at,
            },

            (
                ConversionConfirmed {
                    filled_amount,
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
                direction: direction.clone(),
                amount: *amount,
                filled_amount: *filled_amount,
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
                direction: direction.clone(),
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
                    filled_amount,
                    initiated_at,
                    ..
                },
            ) => Self::WithdrawalSubmitting {
                direction: direction.clone(),
                amount: *filled_amount,
                from_block: *from_block,
                initiated_at: *initiated_at,
            },

            (
                Initiated { withdrawal_ref, .. },
                Self::WithdrawalSubmitting {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::Withdrawing {
                direction: direction.clone(),
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *initiated_at,
            },

            (
                Initiated { withdrawal_ref, .. },
                Self::ConversionComplete {
                    direction,
                    filled_amount,
                    initiated_at,
                    ..
                },
            ) => Self::Withdrawing {
                direction: direction.clone(),
                amount: *filled_amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *initiated_at,
            },

            (
                WithdrawalConfirmed { confirmed_at },
                Self::Withdrawing {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::WithdrawalComplete {
                direction: direction.clone(),
                amount: *amount,
                initiated_at: *initiated_at,
                confirmed_at: *confirmed_at,
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
                direction: direction.clone(),
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            (
                BridgingSubmitting { from_block, .. },
                Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::BridgingSubmitting {
                direction: direction.clone(),
                amount: *amount,
                from_block: *from_block,
                initiated_at: *initiated_at,
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
                    ..
                }
                | Self::WithdrawalComplete {
                    direction,
                    amount,
                    initiated_at,
                    ..
                },
            ) => Self::Bridging {
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
                amount: *amount,
                amount_received: *amount_received,
                fee_collected: *fee_collected,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                initiated_at: *initiated_at,
                minted_at: *minted_at,
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
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
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
                direction: direction.clone(),
                amount: *amount,
                burn_tx_hash: *burn_tx_hash,
                mint_tx_hash: *mint_tx_hash,
                deposit_ref: deposit_ref.clone(),
                reason: reason.clone(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            _ => return Ok(None),
        };

        Ok(Some(next))
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
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

            BeginWithdrawal {
                direction,
                amount,
                from_block,
            } => {
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
                Ok(vec![WithdrawalSubmitting {
                    direction,
                    amount,
                    from_block,
                    submitting_at: Utc::now(),
                }])
            }

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

            ConfirmConversion { .. } | FailConversion { .. } => {
                Err(UsdcRebalanceError::ConversionNotInitiated)
            }

            InitiatePostDepositConversion { .. } => Err(UsdcRebalanceError::DepositNotConfirmed),

            ConfirmWithdrawal | FailWithdrawal { .. } => {
                Err(UsdcRebalanceError::WithdrawalNotInitiated)
            }

            BeginBridging { .. } | InitiateBridging { .. } => {
                Err(UsdcRebalanceError::WithdrawalNotConfirmed)
            }

            ReceiveAttestation { .. } | TimeoutAttestation { .. } | FailBridging { .. } => {
                Err(UsdcRebalanceError::BridgingNotInitiated)
            }

            ConfirmBridging { .. } => Err(UsdcRebalanceError::AttestationNotReceived),

            InitiateDeposit { .. } => Err(UsdcRebalanceError::BridgingNotCompleted),

            ConfirmDeposit | FailDeposit { .. } => Err(UsdcRebalanceError::DepositNotInitiated),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use UsdcRebalanceCommand::*;
        match command {
            InitiateConversion { .. } => Err(UsdcRebalanceError::AlreadyInitiated),
            ConfirmConversion { filled_amount } => {
                self.transition_confirm_conversion(filled_amount)
            }
            FailConversion { reason } => self.transition_fail_conversion(reason),
            InitiatePostDepositConversion { order_id, amount } => {
                self.transition_post_deposit_conversion(order_id, amount)
            }
            BeginWithdrawal {
                direction,
                amount,
                from_block,
            } => self.transition_begin_withdrawal(direction, amount, from_block),
            Initiate {
                direction,
                amount,
                withdrawal,
            } => self.transition_initiate_withdrawal(direction, amount, withdrawal),
            ConfirmWithdrawal => self.transition_confirm_withdrawal(),
            FailWithdrawal { reason } => self.transition_fail_withdrawal(reason),
            BeginBridging { from_block } => self.transition_begin_bridging(from_block),
            InitiateBridging { burn_tx } => self.transition_initiate_bridging(burn_tx),
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
            ),
            TimeoutAttestation { retry_deadline_at } => {
                self.transition_timeout_attestation(retry_deadline_at)
            }
            ConfirmBridging {
                mint_tx,
                amount_received,
                fee_collected,
            } => self.transition_confirm_bridging(mint_tx, amount_received, fee_collected),
            FailBridging { reason } => self.transition_fail_bridging(reason),
            InitiateDeposit { deposit } => self.transition_initiate_deposit(deposit),
            ConfirmDeposit => self.transition_confirm_deposit(),
            FailDeposit { reason } => self.transition_fail_deposit(reason),
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
        filled_amount: Usdc,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { direction, .. } => Ok(vec![ConversionConfirmed {
                direction: direction.clone(),
                filled_amount,
                converted_at: Utc::now(),
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
            direction: direction.clone(),
            amount: *amount,
            order_id,
            initiated_at: Utc::now(),
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
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let Self::ConversionComplete {
            direction: conv_direction,
            filled_amount: conv_filled_amount,
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
        if amount != *conv_filled_amount {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "BeginWithdrawal".to_string(),
                state: format!(
                    "ConversionComplete with amount \
                     mismatch: expected {:?}, got {:?}",
                    conv_filled_amount.inner(),
                    amount.inner()
                ),
            });
        }
        Ok(vec![WithdrawalSubmitting {
            direction,
            amount: *conv_filled_amount,
            from_block,
            submitting_at: Utc::now(),
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
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        let (expected_direction, expected_amount) = match self {
            Self::WithdrawalSubmitting {
                direction, amount, ..
            } => (direction, amount),
            Self::ConversionComplete {
                direction,
                filled_amount,
                ..
            } => (direction, filled_amount),
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
            initiated_at: Utc::now(),
        }])
    }

    fn transition_confirm_withdrawal(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Withdrawing { .. } => Ok(vec![WithdrawalConfirmed {
                confirmed_at: Utc::now(),
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
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::WithdrawalSubmitting { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            Self::WithdrawalComplete { .. } => Ok(vec![BridgingSubmitting {
                from_block,
                submitting_at: Utc::now(),
            }]),
            Self::BridgingSubmitting { .. }
            | Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::InvalidCommand {
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
                    burned_at: Utc::now(),
                }])
            }
            Self::Bridging { .. }
            | Self::AwaitingAttestation { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateBridging".to_string(),
                state: "Bridging".to_string(),
            }),
        }
    }

    fn transition_receive_attestation(
        &self,
        attestation: Vec<u8>,
        cctp_nonce: B256,
        message: Vec<u8>,
        mint_scan_from_block: u64,
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
                    attested_at: Utc::now(),
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
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
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
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    fn transition_confirm_bridging(
        &self,
        mint_tx: TxHash,
        amount_received: Usdc,
        fee_collected: Usdc,
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
                minted_at: Utc::now(),
            }]),
            Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
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
            // Pre-burn failure: withdrawal succeeded and (for BridgingSubmitting)
            // the burn intent was recorded, but no burn tx exists yet -- e.g. a
            // USDC-to-U256 conversion error or a burn that failed before the
            // BridgingInitiated event was persisted.
            Self::WithdrawalComplete { .. } | Self::BridgingSubmitting { .. } => {
                Ok(vec![BridgingFailed {
                    burn_tx_hash: None,
                    cctp_nonce: None,
                    reason,
                    failed_at: Utc::now(),
                }])
            }
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
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::BridgingAlreadyCompleted),
        }
    }

    fn transition_initiate_deposit(
        &self,
        deposit: TransferRef,
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
                deposit_initiated_at: Utc::now(),
            }]),
            Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateDeposit".to_string(),
                state: format!("{self:?}"),
            }),
        }
    }

    fn transition_confirm_deposit(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
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
                direction: direction.clone(),
                deposit_confirmed_at: Utc::now(),
            }]),
            Self::DepositConfirmed { .. } | Self::DepositFailed { .. } => {
                Err(UsdcRebalanceError::InvalidCommand {
                    command: "ConfirmDeposit".to_string(),
                    state: format!("{self:?}"),
                })
            }
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
            Self::DepositConfirmed { .. } | Self::DepositFailed { .. } => {
                Err(UsdcRebalanceError::InvalidCommand {
                    command: "FailDeposit".to_string(),
                    state: format!("{self:?}"),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::fixed_bytes;
    use serde_json::{from_value, json, to_value};
    use std::collections::HashSet;
    use uuid::Uuid;

    use st0x_event_sorcery::{LifecycleError, TestHarness, replay, test_store};
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

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with(())
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

        let events = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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
        let events = TestHarness::<UsdcRebalance>::with(())
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
        let error = TestHarness::<UsdcRebalance>::with(())
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

        let events = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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
        let events = TestHarness::<UsdcRebalance>::with(())
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
                },
            ])
            .when(UsdcRebalanceCommand::BeginBridging { from_block: 99 })
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

        let events = TestHarness::<UsdcRebalance>::with(())
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
                },
                UsdcRebalanceEvent::BridgingSubmitting {
                    from_block: 99,
                    submitting_at: Utc::now(),
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
            },
            UsdcRebalanceEvent::BridgingSubmitting {
                from_block: 99,
                submitting_at: Utc::now(),
            },
        ])
        .unwrap();

        assert!(
            matches!(state, Some(UsdcRebalance::BridgingSubmitting { from_block, .. }) if from_block == 99),
            "Expected BridgingSubmitting state after intent-first sequence, got {state:?}"
        );
    }

    #[tokio::test]
    async fn test_confirm_withdrawal() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal)
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
        let error = TestHarness::<UsdcRebalance>::with(())
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ConfirmWithdrawal)
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmWithdrawal)
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

        let events = TestHarness::<UsdcRebalance>::with(())
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
        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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
        };
        assert_eq!(
            submitting.resumable_post_burn_transfer(),
            None,
            "BridgingSubmitting is pre-burn and must NOT be re-armed (double-burn risk)",
        );
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_before_bridging() {
        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(500.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(10000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(5000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(withdrawal_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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
        let error = TestHarness::<UsdcRebalance>::with(())
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
            .when(UsdcRebalanceCommand::ConfirmWithdrawal)
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

        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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
    async fn test_deposit_failed_rejects_confirm_deposit() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(100.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let events = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
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
        let filled_amount = Usdc::new(float!(998));

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ConfirmConversion { filled_amount })
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
        let error = TestHarness::<UsdcRebalance>::with(())
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: Usdc::new(float!(998)),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount: Usdc::new(float!(998)),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: Usdc::new(float!(998)),
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

        let events = TestHarness::<UsdcRebalance>::with(())
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
        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount: Usdc::new(float!(998)),
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
        let filled_amount = Usdc::new(float!(998));

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount,
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: filled_amount,
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
        let filled_amount = Usdc::new(float!(998));

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount,
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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
        let error = TestHarness::<UsdcRebalance>::with(())
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

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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

    #[tokio::test]
    async fn test_full_base_to_alpaca_flow_with_post_deposit_conversion() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc::new(float!(1000.00)),
                    withdrawal_ref: TransferRef::OnchainTx(burn_tx),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
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
                filled_amount: Usdc::new(float!(998)),
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

    #[test]
    fn to_dto_preserves_original_initiated_at_when_withdrawal_begins_after_conversion() {
        let id = UsdcRebalanceId(Uuid::new_v4());
        let order_id = ClientOrderId::from_uuid(Uuid::new_v4());
        let original_initiated_at = Utc::now();
        let conversion_completed_at = original_initiated_at + chrono::Duration::seconds(30);
        let state = replay::<UsdcRebalance>(vec![
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(1000.00)),
                order_id,
                initiated_at: original_initiated_at,
            },
            UsdcRebalanceEvent::ConversionConfirmed {
                direction: RebalanceDirection::AlpacaToBase,
                filled_amount: Usdc::new(float!(999.99)),
                converted_at: conversion_completed_at,
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc::new(float!(999.99)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: original_initiated_at + chrono::Duration::seconds(60),
            },
        ])
        .expect("event stream should replay into withdrawing state");
        let state = state.expect("event stream should materialize a UsdcRebalance state");

        let dto = state.to_dto(&id);
        let TransferOperation::UsdcBridge(bridge) = dto else {
            panic!("expected UsdcBridge variant");
        };

        assert!(matches!(bridge.status, UsdcBridgeStatus::Withdrawing));
        assert_eq!(
            bridge.started_at, original_initiated_at,
            "initiated_at should remain anchored to the original rebalance initiation time"
        );
        assert_eq!(bridge.updated_at, original_initiated_at);
    }

    #[test]
    fn conversion_confirmed_on_uninitialized_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![UsdcRebalanceEvent::ConversionConfirmed {
            direction: RebalanceDirection::BaseToAlpaca,
            filled_amount: Usdc::new(float!(998)),
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
            filled_amount: Usdc::new(float!(499)),
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
            filled_amount: Usdc::new(float!(499)),
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
            UsdcBridgeStatus::Failed { failed_at: fa } if fa == failed_at
        ));
        assert_eq!(bridge.started_at, initiated_at);
        assert_eq!(bridge.updated_at, failed_at);
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
                filled_amount: amount,
                initiated_at: now,
                converted_at: now,
            }
            .holds_rebalance_guard()
        );
        assert!(
            !ConversionComplete {
                direction: BaseToAlpaca,
                amount,
                filled_amount: amount,
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

    #[tokio::test]
    async fn interrupted_usdc_rebalance_ids_excludes_clearable_terminals() {
        let pool = crate::test_utils::setup_test_db().await;
        let store = test_store::<UsdcRebalance>(pool.clone(), ());
        let amount = Usdc::new(float!(400.0));

        // Clearable terminal (withdrawal failed, pre-burn) -- must be excluded.
        let withdrawal_failed = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &withdrawal_failed,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &withdrawal_failed,
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "x".to_string(),
                },
            )
            .await
            .unwrap();

        // Post-burn bridge failure -- must be included.
        let bridge_failed = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &bridge_failed,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
            )
            .await
            .unwrap();
        store
            .send(&bridge_failed, UsdcRebalanceCommand::ConfirmWithdrawal)
            .await
            .unwrap();
        store
            .send(
                &bridge_failed,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: BURN_TX },
            )
            .await
            .unwrap();
        store
            .send(
                &bridge_failed,
                UsdcRebalanceCommand::FailBridging {
                    reason: "x".to_string(),
                },
            )
            .await
            .unwrap();

        // Awaiting attestation after a post-burn timeout -- must be included.
        let awaiting_attestation = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &awaiting_attestation,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &awaiting_attestation,
                UsdcRebalanceCommand::ConfirmWithdrawal,
            )
            .await
            .unwrap();
        store
            .send(
                &awaiting_attestation,
                UsdcRebalanceCommand::InitiateBridging { burn_tx: BURN_TX },
            )
            .await
            .unwrap();
        store
            .send(
                &awaiting_attestation,
                UsdcRebalanceCommand::TimeoutAttestation {
                    retry_deadline_at: Utc::now() + chrono::Duration::hours(24),
                },
            )
            .await
            .unwrap();

        // In-progress (mid-withdrawal) -- must be included.
        let withdrawing = UsdcRebalanceId(Uuid::new_v4());
        store
            .send(
                &withdrawing,
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount,
                    withdrawal: TransferRef::OnchainTx(BURN_TX),
                },
            )
            .await
            .unwrap();

        let interrupted = interrupted_usdc_rebalance_ids(&pool).await.unwrap();
        let got: HashSet<UsdcRebalanceId> = interrupted.ids.into_iter().collect();

        assert_eq!(
            got,
            HashSet::from([bridge_failed, awaiting_attestation, withdrawing])
        );
        assert!(
            interrupted.unparseable.is_empty(),
            "well-formed UUID aggregate_ids must not be reported as unparseable"
        );
    }
}
