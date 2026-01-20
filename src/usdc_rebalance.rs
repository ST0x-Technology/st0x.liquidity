//! USDC Rebalance aggregate for cross-chain USDC transfers between Alpaca and Base.
//!
//! This module implements the CQRS-ES aggregate pattern for managing the asynchronous workflow
//! of rebalancing USDC between Alpaca (offchain) and Base (onchain) via Circle's Cross-Chain
//! Transfer Protocol (CCTP).
//!
//! # State Flow
//!
//! The aggregate progresses through 10 states grouped into three phases:
//!
//! ```text
//! WITHDRAWAL PHASE:
//!   Uninitialized --Initiate--> Withdrawing --ConfirmWithdrawal--> WithdrawalComplete
//!                                    |                                     |
//!                                    +--FailWithdrawal--> WithdrawalFailed |
//!                                                                          |
//! BRIDGING PHASE:                                            InitiateBridging
//!                                                                          |
//!                                                                          v
//!   Bridging --ReceiveAttestation--> Attested --ConfirmBridging--> Bridged
//!       |                                |                             |
//!       +----------FailBridging----------+--> BridgingFailed           |
//!                                                                      |
//! DEPOSIT PHASE:                                             InitiateDeposit
//!                                                                      |
//!                                                                      v
//!   DepositInitiated --ConfirmDeposit--> DepositConfirmed (success)
//!          |
//!          +--FailDeposit--> DepositFailed (failure)
//!
//! Terminal states: WithdrawalFailed, BridgingFailed, DepositFailed, DepositConfirmed
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
//! Terminal states (`*Failed`, `DepositConfirmed`) reject all commands to prevent
//! invalid state transitions.
//!
//! [`AlpacaWalletService`]: crate::alpaca_wallet::AlpacaWalletService

use alloy::primitives::TxHash;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteEventRepository;
use uuid::Uuid;

use crate::alpaca_wallet::AlpacaTransferId;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::threshold::Usdc;

/// SQLite-backed event store for UsdcRebalance aggregates.
pub(crate) type UsdcEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<UsdcRebalance, Never>>;

/// Unique identifier for a USDC rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct UsdcRebalanceId(pub(crate) String);

impl UsdcRebalanceId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
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
#[derive(Debug, thiserror::Error)]
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
    /// Withdrawal has not been initiated yet
    #[error("Withdrawal has not been initiated")]
    WithdrawalNotInitiated,
    /// Withdrawal has already been confirmed or failed
    #[error("Withdrawal has already completed")]
    WithdrawalAlreadyCompleted,
    /// Withdrawal has not been confirmed yet
    #[error("Withdrawal has not been confirmed")]
    WithdrawalNotConfirmed,
    /// Conversion has not been completed yet
    #[error("Conversion has not been completed")]
    ConversionNotCompleted,
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
    /// Lifecycle state error
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
}

/// Commands for the USDC rebalance aggregate.
///
/// Commands are validated against the current state before being processed.
/// Invalid commands return appropriate errors without mutating state.
///
/// # Conversion Commands
///
/// There are two conversion commands because conversion happens at different points in each flow:
/// - **AlpacaToBase**: Convert USD→USDC BEFORE withdrawal (need USDC for CCTP bridge)
/// - **BaseToAlpaca**: Convert USDC→USD AFTER deposit (USDC arrives in crypto wallet)
#[derive(Debug, Clone)]
pub(crate) enum UsdcRebalanceCommand {
    /// Start pre-withdrawal conversion for AlpacaToBase direction.
    /// Converts USD buying power to USDC before withdrawal to Alpaca's crypto wallet.
    /// Valid only from `Uninitialized` state.
    InitiateConversion {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: Uuid,
    },
    /// Confirm successful conversion. Valid only from `Converting` state.
    ConfirmConversion,
    /// Record conversion failure. Valid only from `Converting` state.
    FailConversion { reason: String },
    /// Start post-deposit conversion for BaseToAlpaca direction.
    /// Converts USDC (deposited via CCTP) to USD buying power for trading.
    /// Valid only from `DepositConfirmed` state.
    InitiatePostDepositConversion { order_id: Uuid },
    /// Start a new rebalancing operation. Valid only from `Uninitialized` state or
    /// `ConversionComplete` state (for AlpacaToBase direction).
    Initiate {
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
    },
    /// Confirm successful withdrawal from source. Valid only from `Withdrawing` state.
    ConfirmWithdrawal,
    /// Record the CCTP burn transaction. Valid only from `WithdrawalComplete` state.
    InitiateBridging { burn_tx: TxHash },
    /// Record the Circle attestation. Valid only from `Bridging` state.
    /// The cctp_nonce is extracted from the attested message (not the burn tx, which has placeholder).
    ReceiveAttestation {
        attestation: Vec<u8>,
        cctp_nonce: u64,
    },
    /// Confirm the CCTP mint transaction. Valid only from `Attested` state.
    ConfirmBridging { mint_tx: TxHash },
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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum UsdcRebalanceEvent {
    /// Conversion operation started (USD<->USDC). Records direction, amount, and order ID.
    ConversionInitiated {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: Uuid,
        initiated_at: DateTime<Utc>,
    },
    /// Conversion completed successfully.
    ConversionConfirmed { converted_at: DateTime<Utc> },
    /// Conversion failed.
    ConversionFailed {
        reason: String,
        failed_at: DateTime<Utc>,
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
    /// CCTP burn transaction submitted. Records burn hash for attestation lookup.
    BridgingInitiated {
        burn_tx_hash: TxHash,
        burned_at: DateTime<Utc>,
    },
    /// Circle attestation received. Enables minting on destination chain.
    /// The cctp_nonce is extracted from the attested message (the real nonce, not the placeholder).
    BridgeAttestationReceived {
        attestation: Vec<u8>,
        cctp_nonce: u64,
        attested_at: DateTime<Utc>,
    },
    /// CCTP mint transaction confirmed on destination chain.
    Bridged {
        mint_tx_hash: TxHash,
        minted_at: DateTime<Utc>,
    },
    /// Bridging failed. Preserves burn data when available for debugging.
    BridgingFailed {
        burn_tx_hash: Option<TxHash>,
        cctp_nonce: Option<u64>,
        reason: String,
        failed_at: DateTime<Utc>,
    },
    /// Deposit to destination initiated.
    DepositInitiated {
        deposit_ref: TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    },
    /// Deposit completed successfully. Terminal success state.
    DepositConfirmed { deposit_confirmed_at: DateTime<Utc> },
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
            Self::Initiated { .. } => "UsdcRebalanceEvent::Initiated",
            Self::WithdrawalConfirmed { .. } => "UsdcRebalanceEvent::WithdrawalConfirmed",
            Self::WithdrawalFailed { .. } => "UsdcRebalanceEvent::WithdrawalFailed",
            Self::BridgingInitiated { .. } => "UsdcRebalanceEvent::BridgingInitiated",
            Self::BridgeAttestationReceived { .. } => {
                "UsdcRebalanceEvent::BridgeAttestationReceived"
            }
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
        order_id: Uuid,
        initiated_at: DateTime<Utc>,
    },
    /// Conversion has completed, ready for next phase
    ConversionComplete {
        direction: RebalanceDirection,
        amount: Usdc,
        initiated_at: DateTime<Utc>,
        converted_at: DateTime<Utc>,
    },
    /// Conversion has failed (terminal state)
    ConversionFailed {
        direction: RebalanceDirection,
        amount: Usdc,
        order_id: Uuid,
        reason: String,
        initiated_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
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
    /// CCTP bridging has been initiated (burn transaction submitted)
    /// Note: cctp_nonce is not available here - it's only known after attestation.
    Bridging {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        initiated_at: DateTime<Utc>,
        burned_at: DateTime<Utc>,
    },
    /// Circle attestation has been received, ready for minting on destination chain
    Attested {
        direction: RebalanceDirection,
        amount: Usdc,
        burn_tx_hash: TxHash,
        cctp_nonce: u64,
        attestation: Vec<u8>,
        initiated_at: DateTime<Utc>,
        attested_at: DateTime<Utc>,
    },
    /// USDC has been minted on destination chain via CCTP
    Bridged {
        direction: RebalanceDirection,
        amount: Usdc,
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
        cctp_nonce: Option<u64>,
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

#[async_trait]
impl Aggregate for Lifecycle<UsdcRebalance, Never> {
    type Command = UsdcRebalanceCommand;
    type Event = UsdcRebalanceEvent;
    type Error = UsdcRebalanceError;
    type Services = ();

    fn aggregate_type() -> String {
        "UsdcRebalance".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, UsdcRebalance::apply_transition)
            .or_initialize(&event, UsdcRebalance::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            UsdcRebalanceCommand::InitiateConversion {
                direction,
                amount,
                order_id,
            } => self.handle_initiate_conversion(direction, *amount, *order_id),

            UsdcRebalanceCommand::ConfirmConversion => self.handle_confirm_conversion(),

            UsdcRebalanceCommand::FailConversion { reason } => self.handle_fail_conversion(reason),

            UsdcRebalanceCommand::InitiatePostDepositConversion { order_id } => {
                self.handle_initiate_post_deposit_conversion(*order_id)
            }

            UsdcRebalanceCommand::Initiate {
                direction,
                amount,
                withdrawal,
            } => self.handle_initiate(direction, *amount, withdrawal),

            UsdcRebalanceCommand::ConfirmWithdrawal => self.handle_confirm_withdrawal(),

            UsdcRebalanceCommand::FailWithdrawal { reason } => self.handle_fail_withdrawal(reason),

            UsdcRebalanceCommand::InitiateBridging { burn_tx } => {
                self.handle_initiate_bridging(burn_tx)
            }

            UsdcRebalanceCommand::ReceiveAttestation {
                attestation,
                cctp_nonce,
            } => self.handle_receive_attestation(attestation, *cctp_nonce),

            UsdcRebalanceCommand::ConfirmBridging { mint_tx } => {
                self.handle_confirm_bridging(mint_tx)
            }

            UsdcRebalanceCommand::FailBridging { reason } => self.handle_fail_bridging(reason),

            UsdcRebalanceCommand::InitiateDeposit { deposit } => {
                self.handle_initiate_deposit(deposit)
            }

            UsdcRebalanceCommand::ConfirmDeposit => self.handle_confirm_deposit(),

            UsdcRebalanceCommand::FailDeposit { reason } => self.handle_fail_deposit(reason),
        }
    }
}

impl Lifecycle<UsdcRebalance, Never> {
    fn handle_initiate_conversion(
        &self,
        direction: &RebalanceDirection,
        amount: Usdc,
        order_id: Uuid,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => {
                Ok(vec![UsdcRebalanceEvent::ConversionInitiated {
                    direction: direction.clone(),
                    amount,
                    order_id,
                    initiated_at: Utc::now(),
                }])
            }
            Ok(_) => Err(UsdcRebalanceError::AlreadyInitiated),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_confirm_conversion(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Ok(UsdcRebalance::Converting { .. }) => {
                Ok(vec![UsdcRebalanceEvent::ConversionConfirmed {
                    converted_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::ConversionComplete { .. } | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcRebalanceError::ConversionAlreadyCompleted),
            Err(e) if !matches!(e, LifecycleError::Uninitialized) => Err(e.into()),
            _ => Err(UsdcRebalanceError::ConversionNotInitiated),
        }
    }

    fn handle_fail_conversion(
        &self,
        reason: &str,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Ok(UsdcRebalance::Converting { .. }) => {
                Ok(vec![UsdcRebalanceEvent::ConversionFailed {
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::ConversionComplete { .. } | UsdcRebalance::ConversionFailed { .. },
            ) => Err(UsdcRebalanceError::ConversionAlreadyCompleted),
            Err(e) if !matches!(e, LifecycleError::Uninitialized) => Err(e.into()),
            _ => Err(UsdcRebalanceError::ConversionNotInitiated),
        }
    }

    fn handle_initiate_post_deposit_conversion(
        &self,
        order_id: Uuid,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Ok(UsdcRebalance::DepositConfirmed {
                direction, amount, ..
            }) => {
                if *direction != RebalanceDirection::BaseToAlpaca {
                    return Err(UsdcRebalanceError::WrongDirectionForPostDepositConversion);
                }
                Ok(vec![UsdcRebalanceEvent::ConversionInitiated {
                    direction: direction.clone(),
                    amount: *amount,
                    order_id,
                    initiated_at: Utc::now(),
                }])
            }
            Err(e) if !matches!(e, LifecycleError::Uninitialized) => Err(e.into()),
            _ => Err(UsdcRebalanceError::DepositNotConfirmed),
        }
    }

    fn handle_initiate(
        &self,
        direction: &RebalanceDirection,
        amount: Usdc,
        withdrawal: &TransferRef,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Ok(vec![UsdcRebalanceEvent::Initiated {
                direction: direction.clone(),
                amount,
                withdrawal_ref: withdrawal.clone(),
                initiated_at: Utc::now(),
            }]),
            Ok(UsdcRebalance::ConversionComplete {
                direction: conv_direction,
                amount: conv_amount,
                ..
            }) => {
                if direction != conv_direction {
                    return Err(UsdcRebalanceError::InvalidCommand {
                        command: "Initiate".to_string(),
                        state: "ConversionComplete with different direction".to_string(),
                    });
                }
                Ok(vec![UsdcRebalanceEvent::Initiated {
                    direction: direction.clone(),
                    amount: *conv_amount,
                    withdrawal_ref: withdrawal.clone(),
                    initiated_at: Utc::now(),
                }])
            }
            Ok(_) => Err(UsdcRebalanceError::AlreadyInitiated),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_confirm_withdrawal(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Ok(UsdcRebalance::Withdrawing { .. }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            Err(e) if !matches!(e, LifecycleError::Uninitialized) => Err(e.into()),
            _ => Err(UsdcRebalanceError::WithdrawalNotInitiated),
        }
    }

    fn handle_fail_withdrawal(
        &self,
        reason: &str,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Ok(UsdcRebalance::Withdrawing { .. }) => {
                Ok(vec![UsdcRebalanceEvent::WithdrawalFailed {
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }
            Ok(
                UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            Err(e) if !matches!(e, LifecycleError::Uninitialized) => Err(e.into()),
            _ => Err(UsdcRebalanceError::WithdrawalNotInitiated),
        }
    }

    fn handle_initiate_bridging(
        &self,
        burn_tx: &TxHash,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalFailed { .. },
            ) => Err(UsdcRebalanceError::WithdrawalNotConfirmed),

            Ok(UsdcRebalance::WithdrawalComplete { .. }) => {
                Ok(vec![UsdcRebalanceEvent::BridgingInitiated {
                    burn_tx_hash: *burn_tx,
                    burned_at: Utc::now(),
                }])
            }

            Ok(
                UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateBridging".to_string(),
                state: "Bridging".to_string(),
            }),

            Err(e) => Err(e.into()),
        }
    }

    fn handle_receive_attestation(
        &self,
        attestation: &[u8],
        cctp_nonce: u64,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingNotInitiated),

            Ok(UsdcRebalance::Bridging { .. }) => {
                Ok(vec![UsdcRebalanceEvent::BridgeAttestationReceived {
                    attestation: attestation.to_vec(),
                    cctp_nonce,
                    attested_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::Attested { .. }) => Err(UsdcRebalanceError::InvalidCommand {
                command: "ReceiveAttestation".to_string(),
                state: "Attested".to_string(),
            }),

            Ok(
                UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingAlreadyCompleted),

            Err(e) => Err(e.into()),
        }
    }

    fn handle_confirm_bridging(
        &self,
        mint_tx: &TxHash,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. },
            ) => Err(UsdcRebalanceError::AttestationNotReceived),

            Ok(UsdcRebalance::Attested { .. }) => Ok(vec![UsdcRebalanceEvent::Bridged {
                mint_tx_hash: *mint_tx,
                minted_at: Utc::now(),
            }]),

            Ok(
                UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingAlreadyCompleted),

            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail_bridging(
        &self,
        reason: &str,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingNotInitiated),

            Ok(UsdcRebalance::Bridging { burn_tx_hash, .. }) => {
                Ok(vec![UsdcRebalanceEvent::BridgingFailed {
                    burn_tx_hash: Some(*burn_tx_hash),
                    cctp_nonce: None,
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::Attested {
                burn_tx_hash,
                cctp_nonce,
                ..
            }) => Ok(vec![UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(*burn_tx_hash),
                cctp_nonce: Some(*cctp_nonce),
                reason: reason.to_string(),
                failed_at: Utc::now(),
            }]),

            Ok(
                UsdcRebalance::Bridged { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingAlreadyCompleted),

            Err(e) => Err(e.into()),
        }
    }

    fn handle_initiate_deposit(
        &self,
        deposit: &TransferRef,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::BridgingFailed { .. },
            ) => Err(UsdcRebalanceError::BridgingNotCompleted),

            Ok(UsdcRebalance::Bridged { .. }) => Ok(vec![UsdcRebalanceEvent::DepositInitiated {
                deposit_ref: deposit.clone(),
                deposit_initiated_at: Utc::now(),
            }]),

            Ok(
                UsdcRebalance::DepositInitiated { .. }
                | UsdcRebalance::DepositConfirmed { .. }
                | UsdcRebalance::DepositFailed { .. },
            ) => Err(UsdcRebalanceError::InvalidCommand {
                command: "InitiateDeposit".to_string(),
                state: format!("{:?}", self.live()),
            }),

            Err(e) => Err(e.into()),
        }
    }

    fn handle_confirm_deposit(&self) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::Bridged { .. },
            ) => Err(UsdcRebalanceError::DepositNotInitiated),

            Ok(UsdcRebalance::DepositInitiated { .. }) => {
                Ok(vec![UsdcRebalanceEvent::DepositConfirmed {
                    deposit_confirmed_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::DepositConfirmed { .. } | UsdcRebalance::DepositFailed { .. }) => {
                Err(UsdcRebalanceError::InvalidCommand {
                    command: "ConfirmDeposit".to_string(),
                    state: format!("{:?}", self.live()),
                })
            }

            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail_deposit(
        &self,
        reason: &str,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                UsdcRebalance::Converting { .. }
                | UsdcRebalance::ConversionComplete { .. }
                | UsdcRebalance::ConversionFailed { .. }
                | UsdcRebalance::Withdrawing { .. }
                | UsdcRebalance::WithdrawalComplete { .. }
                | UsdcRebalance::WithdrawalFailed { .. }
                | UsdcRebalance::Bridging { .. }
                | UsdcRebalance::Attested { .. }
                | UsdcRebalance::BridgingFailed { .. }
                | UsdcRebalance::Bridged { .. },
            ) => Err(UsdcRebalanceError::DepositNotInitiated),

            Ok(UsdcRebalance::DepositInitiated { deposit_ref, .. }) => {
                Ok(vec![UsdcRebalanceEvent::DepositFailed {
                    deposit_ref: Some(deposit_ref.clone()),
                    reason: reason.to_string(),
                    failed_at: Utc::now(),
                }])
            }

            Ok(UsdcRebalance::DepositConfirmed { .. } | UsdcRebalance::DepositFailed { .. }) => {
                Err(UsdcRebalanceError::InvalidCommand {
                    command: "FailDeposit".to_string(),
                    state: format!("{:?}", self.live()),
                })
            }

            Err(e) => Err(e.into()),
        }
    }
}

impl UsdcRebalance {
    /// Apply a transition event to an existing rebalance state.
    pub(crate) fn apply_transition(
        event: &UsdcRebalanceEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            UsdcRebalanceEvent::ConversionConfirmed { converted_at } => {
                current.apply_conversion_confirmed(*converted_at)
            }
            UsdcRebalanceEvent::ConversionFailed { reason, failed_at } => {
                current.apply_conversion_failed(reason, *failed_at)
            }
            UsdcRebalanceEvent::Initiated {
                withdrawal_ref,
                initiated_at,
                ..
            } => current.apply_initiated(withdrawal_ref, *initiated_at),
            UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at } => {
                current.apply_withdrawal_confirmed(*confirmed_at)
            }
            UsdcRebalanceEvent::WithdrawalFailed { reason, failed_at } => {
                current.apply_withdrawal_failed(reason, *failed_at)
            }
            UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                burned_at,
            } => current.apply_bridging_initiated(*burn_tx_hash, *burned_at),
            UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation,
                cctp_nonce,
                attested_at,
            } => current.apply_attestation_received(attestation, *cctp_nonce, *attested_at),
            UsdcRebalanceEvent::Bridged {
                mint_tx_hash,
                minted_at,
            } => current.apply_bridged(*mint_tx_hash, *minted_at),
            UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash,
                cctp_nonce,
                reason,
                failed_at,
            } => current.apply_bridging_failed(*burn_tx_hash, *cctp_nonce, reason, *failed_at),
            UsdcRebalanceEvent::DepositInitiated {
                deposit_ref,
                deposit_initiated_at,
            } => current.apply_deposit_initiated(deposit_ref, *deposit_initiated_at),
            UsdcRebalanceEvent::DepositConfirmed {
                deposit_confirmed_at,
            } => current.apply_deposit_confirmed(*deposit_confirmed_at),
            UsdcRebalanceEvent::DepositFailed {
                deposit_ref,
                reason,
                failed_at,
            } => current.apply_deposit_failed(deposit_ref.as_ref(), reason, *failed_at),
            UsdcRebalanceEvent::ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at,
            } => current.apply_conversion_initiated(direction, *amount, *order_id, *initiated_at),
        }
    }

    fn apply_conversion_initiated(
        &self,
        direction: &RebalanceDirection,
        amount: Usdc,
        order_id: Uuid,
        initiated_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::DepositConfirmed { .. } = self else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "ConversionInitiated".to_string(),
            });
        };

        Ok(Self::Converting {
            direction: direction.clone(),
            amount,
            order_id,
            initiated_at,
        })
    }

    fn apply_conversion_confirmed(
        &self,
        converted_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Converting {
            direction,
            amount,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "ConversionConfirmed".to_string(),
            });
        };

        Ok(Self::ConversionComplete {
            direction: direction.clone(),
            amount: *amount,
            initiated_at: *initiated_at,
            converted_at,
        })
    }

    fn apply_conversion_failed(
        &self,
        reason: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Converting {
            direction,
            amount,
            order_id,
            initiated_at,
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "ConversionFailed".to_string(),
            });
        };

        Ok(Self::ConversionFailed {
            direction: direction.clone(),
            amount: *amount,
            order_id: *order_id,
            reason: reason.to_string(),
            initiated_at: *initiated_at,
            failed_at,
        })
    }

    fn apply_initiated(
        &self,
        withdrawal_ref: &TransferRef,
        initiated_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::ConversionComplete {
            direction, amount, ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "Initiated".to_string(),
            });
        };

        Ok(Self::Withdrawing {
            direction: direction.clone(),
            amount: *amount,
            withdrawal_ref: withdrawal_ref.clone(),
            initiated_at,
        })
    }

    fn apply_withdrawal_confirmed(
        &self,
        confirmed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Withdrawing {
            direction,
            amount,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "WithdrawalConfirmed".to_string(),
            });
        };

        Ok(Self::WithdrawalComplete {
            direction: direction.clone(),
            amount: *amount,
            initiated_at: *initiated_at,
            confirmed_at,
        })
    }

    fn apply_withdrawal_failed(
        &self,
        reason: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Withdrawing {
            direction,
            amount,
            withdrawal_ref,
            initiated_at,
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "WithdrawalFailed".to_string(),
            });
        };

        Ok(Self::WithdrawalFailed {
            direction: direction.clone(),
            amount: *amount,
            withdrawal_ref: withdrawal_ref.clone(),
            reason: reason.to_string(),
            initiated_at: *initiated_at,
            failed_at,
        })
    }

    fn apply_bridging_initiated(
        &self,
        burn_tx_hash: TxHash,
        burned_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::WithdrawalComplete {
            direction,
            amount,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "BridgingInitiated".to_string(),
            });
        };

        Ok(Self::Bridging {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash,
            initiated_at: *initiated_at,
            burned_at,
        })
    }

    fn apply_attestation_received(
        &self,
        attestation: &[u8],
        cctp_nonce: u64,
        attested_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Bridging {
            direction,
            amount,
            burn_tx_hash,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "BridgeAttestationReceived".to_string(),
            });
        };

        Ok(Self::Attested {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash: *burn_tx_hash,
            cctp_nonce,
            attestation: attestation.to_vec(),
            initiated_at: *initiated_at,
            attested_at,
        })
    }

    fn apply_bridged(
        &self,
        mint_tx_hash: TxHash,
        minted_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Attested {
            direction,
            amount,
            burn_tx_hash,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "Bridged".to_string(),
            });
        };

        Ok(Self::Bridged {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash: *burn_tx_hash,
            mint_tx_hash,
            initiated_at: *initiated_at,
            minted_at,
        })
    }

    fn apply_bridging_failed(
        &self,
        burn_tx_hash: Option<TxHash>,
        cctp_nonce: Option<u64>,
        reason: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let (Self::Bridging {
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
        }) = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "BridgingFailed".to_string(),
            });
        };

        Ok(Self::BridgingFailed {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash,
            cctp_nonce,
            reason: reason.to_string(),
            initiated_at: *initiated_at,
            failed_at,
        })
    }

    fn apply_deposit_initiated(
        &self,
        deposit_ref: &TransferRef,
        deposit_initiated_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::Bridged {
            direction,
            amount,
            burn_tx_hash,
            mint_tx_hash,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "DepositInitiated".to_string(),
            });
        };

        Ok(Self::DepositInitiated {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash: *burn_tx_hash,
            mint_tx_hash: *mint_tx_hash,
            deposit_ref: deposit_ref.clone(),
            initiated_at: *initiated_at,
            deposit_initiated_at,
        })
    }

    fn apply_deposit_confirmed(
        &self,
        deposit_confirmed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::DepositInitiated {
            direction,
            amount,
            burn_tx_hash,
            mint_tx_hash,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "DepositConfirmed".to_string(),
            });
        };

        Ok(Self::DepositConfirmed {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash: *burn_tx_hash,
            mint_tx_hash: *mint_tx_hash,
            initiated_at: *initiated_at,
            deposit_confirmed_at,
        })
    }

    fn apply_deposit_failed(
        &self,
        deposit_ref: Option<&TransferRef>,
        reason: &str,
        failed_at: DateTime<Utc>,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::DepositInitiated {
            direction,
            amount,
            burn_tx_hash,
            mint_tx_hash,
            initiated_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: "DepositFailed".to_string(),
            });
        };

        Ok(Self::DepositFailed {
            direction: direction.clone(),
            amount: *amount,
            burn_tx_hash: *burn_tx_hash,
            mint_tx_hash: *mint_tx_hash,
            deposit_ref: deposit_ref.cloned(),
            reason: reason.to_string(),
            initiated_at: *initiated_at,
            failed_at,
        })
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(event: &UsdcRebalanceEvent) -> Result<Self, LifecycleError<Never>> {
        match event {
            UsdcRebalanceEvent::ConversionInitiated {
                direction,
                amount,
                order_id,
                initiated_at,
            } => Ok(Self::Converting {
                direction: direction.clone(),
                amount: *amount,
                order_id: *order_id,
                initiated_at: *initiated_at,
            }),

            UsdcRebalanceEvent::Initiated {
                direction,
                amount,
                withdrawal_ref,
                initiated_at,
            } => Ok(Self::Withdrawing {
                direction: direction.clone(),
                amount: *amount,
                withdrawal_ref: withdrawal_ref.clone(),
                initiated_at: *initiated_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: event.event_type(),
            }),
        }
    }
}

impl View<Self> for Lifecycle<UsdcRebalance, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, UsdcRebalance::apply_transition)
            .or_initialize(&event.payload, UsdcRebalance::from_event);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::fixed_bytes;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));

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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*withdrawal_ref, TransferRef::AlpacaId(transfer_id));
    }

    #[tokio::test]
    async fn test_initiate_base_to_alpaca() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(500.50)),
                    withdrawal: TransferRef::OnchainTx(tx_hash),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));

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
        assert_eq!(*amount, Usdc(dec!(500.50)));
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        };
        aggregate.apply(event);

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(500.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::AlreadyInitiated
        ));
    }

    #[test]
    fn test_view_tracks_initiation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&envelope);

        let Lifecycle::Live(UsdcRebalance::Withdrawing {
            direction,
            amount,
            withdrawal_ref,
            initiated_at: view_initiated_at,
        }) = view
        else {
            panic!("Expected Live(Withdrawing) variant");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(view_initiated_at, initiated_at);
    }

    #[test]
    fn test_from_event_rejects_non_init_events() {
        let event = UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        };

        let result = UsdcRebalance::from_event(&event);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[test]
    fn test_apply_transition_rejects_initiated_event() {
        let current = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        };

        let event = UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(500.00)),
            withdrawal_ref: TransferRef::OnchainTx(fixed_bytes!(
                "0x0000000000000000000000000000000000000000000000000000000000000001"
            )),
            initiated_at: Utc::now(),
        };

        let result = UsdcRebalance::apply_transition(&event, &current);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[tokio::test]
    async fn test_confirm_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::WithdrawalConfirmed { .. }
        ));

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::WithdrawalComplete {
            direction,
            amount,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected WithdrawalComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_withdrawal_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_fail_withdrawal_after_initiation() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Insufficient funds".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::WithdrawalFailed { reason, .. } = &events[0] else {
            panic!("Expected WithdrawalFailed event");
        };
        assert_eq!(reason, "Insufficient funds");

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::WithdrawalFailed {
            direction,
            amount,
            reason: state_reason,
            withdrawal_ref,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected WithdrawalFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_reason, "Insufficient funds");
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_fail_withdrawal_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Test failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_confirmed_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Late failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_failed_withdrawal() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailWithdrawal {
                    reason: "Second failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalAlreadyCompleted
        ));
    }

    #[test]
    fn test_view_tracks_withdrawal_confirmation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::WithdrawalComplete {
            direction,
            amount,
            initiated_at: view_initiated_at,
            confirmed_at: view_confirmed_at,
        }) = view
        else {
            panic!("Expected WithdrawalComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_confirmed_at, confirmed_at);
    }

    #[test]
    fn test_view_tracks_withdrawal_failure() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let failed_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalFailed {
                reason: "Network error".to_string(),
                failed_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::WithdrawalFailed {
            direction,
            amount,
            withdrawal_ref,
            reason,
            initiated_at: view_initiated_at,
            failed_at: view_failed_at,
        }) = view
        else {
            panic!("Expected WithdrawalFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
        assert_eq!(reason, "Network error");
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_failed_at, failed_at);
    }

    #[tokio::test]
    async fn test_initiate_bridging_after_withdrawal_confirmed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: event_tx_hash,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgingInitiated event");
        };

        assert_eq!(*event_tx_hash, burn_tx_hash);

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::Bridging {
            direction,
            amount,
            burn_tx_hash: state_tx_hash,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected Bridging state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_tx_hash, burn_tx_hash);
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_before_withdrawal() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_while_withdrawing() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_after_withdrawal_failed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Test failure".to_string(),
            failed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WithdrawalNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_bridging_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::InvalidCommand { .. }
        ));
    }

    #[test]
    fn test_view_tracks_bridging_initiation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::Bridging {
            direction,
            amount,
            burn_tx_hash: view_tx_hash,
            initiated_at: view_initiated_at,
            burned_at: view_burned_at,
        }) = view
        else {
            panic!("Expected Bridging state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_tx_hash, burn_tx_hash);
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_burned_at, burned_at);
    }

    #[tokio::test]
    async fn test_receive_attestation_after_bridging_initiated() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = 12345u64;

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let attestation = vec![0x01, 0x02, 0x03, 0x04];

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: attestation.clone(),
                    cctp_nonce,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: event_attestation,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgeAttestationReceived event");
        };

        assert_eq!(*event_attestation, attestation);

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::Attested {
            direction,
            amount,
            burn_tx_hash: state_tx_hash,
            cctp_nonce: state_nonce,
            attestation: state_attestation,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected Attested state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_tx_hash, burn_tx_hash);
        assert_eq!(state_nonce, cctp_nonce);
        assert_eq!(state_attestation, attestation);
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_before_bridging() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_while_withdrawing() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_complete() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_after_withdrawal_failed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Test failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01, 0x02],
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x03, 0x04],
                    cctp_nonce: 99999,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::InvalidCommand { .. }
        ));
    }

    #[test]
    fn test_view_tracks_attestation_receipt() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let attested_at = Utc::now();
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = 12345u64;
        let attestation = vec![0x01, 0x02, 0x03, 0x04];

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 4,
            payload: UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: attestation.clone(),
                cctp_nonce,
                attested_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::Attested {
            direction,
            amount,
            burn_tx_hash: view_tx_hash,
            cctp_nonce: view_nonce,
            attestation: view_attestation,
            initiated_at: view_initiated_at,
            attested_at: view_attested_at,
        }) = view
        else {
            panic!("Expected Attested state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_tx_hash, burn_tx_hash);
        assert_eq!(view_nonce, cctp_nonce);
        assert_eq!(view_attestation, attestation);
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_attested_at, attested_at);
    }

    #[tokio::test]
    async fn test_confirm_bridging() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = 12345u64;

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let attestation = vec![0x01, 0x02, 0x03, 0x04];
        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: attestation.clone(),
            cctp_nonce,
            attested_at: Utc::now(),
        });

        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::Bridged {
            mint_tx_hash: event_mint_tx,
            ..
        } = &events[0]
        else {
            panic!("Expected Bridged event");
        };

        assert_eq!(*event_mint_tx, mint_tx_hash);

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::Bridged {
            direction,
            amount,
            burn_tx_hash: state_burn_tx,
            mint_tx_hash: state_mint_tx,
            initiated_at: state_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected Bridged state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*state_burn_tx, burn_tx_hash);
        assert_eq!(*state_mint_tx, mint_tx_hash);
        assert_eq!(*state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_confirm_bridging_before_attestation() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_tx_hash,
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::AttestationNotReceived
        ));
    }

    #[tokio::test]
    async fn test_fail_bridging_after_initiated() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailBridging {
                    reason: "CCTP timeout".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

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
        // cctp_nonce is None when failing from Bridging state (nonce not yet known)
        assert_eq!(*event_nonce, None);
        assert_eq!(reason, "CCTP timeout");

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::BridgingFailed {
            direction,
            amount,
            burn_tx_hash: state_burn_tx,
            cctp_nonce: state_nonce,
            reason: state_reason,
            initiated_at: state_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected BridgingFailed state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*state_burn_tx, Some(burn_tx_hash));
        // cctp_nonce is None when failing from Bridging state (nonce not yet known)
        assert_eq!(*state_nonce, None);
        assert_eq!(state_reason, "CCTP timeout");
        assert_eq!(*state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_fail_bridging_after_attestation_received() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = 12345u64;

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce,
            attested_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailBridging {
                    reason: "Mint transaction failed".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

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
        // cctp_nonce is Some when failing from Attested state (nonce known from attestation)
        assert_eq!(*event_nonce, Some(cctp_nonce));
        assert_eq!(reason, "Mint transaction failed");

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::BridgingFailed {
            direction,
            amount,
            burn_tx_hash: state_burn_tx,
            cctp_nonce: state_nonce,
            reason: state_reason,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected BridgingFailed state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*state_burn_tx, Some(burn_tx_hash));
        // cctp_nonce is Some when failing from Attested state (nonce known from attestation)
        assert_eq!(*state_nonce, Some(cctp_nonce));
        assert_eq!(state_reason, "Mint transaction failed");
    }

    #[tokio::test]
    async fn test_bridging_failed_preserves_burn_data_when_available() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0xabababababababababababababababababababababababababababababababab");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailBridging {
                    reason: "Attestation service down".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

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
        // cctp_nonce is None when failing from Bridging state (not yet known)
        assert_eq!(
            *event_nonce, None,
            "CCTP nonce should be None when failing from Bridging state"
        );
    }

    #[tokio::test]
    async fn test_cannot_confirm_bridging_after_already_completed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: fixed_bytes!(
                        "0x2222222222222222222222222222222222222222222222222222222222222222"
                    ),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_bridging_after_already_completed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailBridging {
                    reason: "Late failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_bridging_after_already_failed() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: Some(burn_tx_hash),
            cctp_nonce: None, // nonce unknown when failing from Bridging state
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailBridging {
                    reason: "Second failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingAlreadyCompleted
        ));
    }

    #[test]
    fn test_view_tracks_bridging_completion() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let attested_at = Utc::now();
        let minted_at = Utc::now();
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let cctp_nonce = 12345u64;

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 4,
            payload: UsdcRebalanceEvent::BridgeAttestationReceived {
                attestation: vec![0x01, 0x02],
                cctp_nonce,
                attested_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 5,
            payload: UsdcRebalanceEvent::Bridged {
                mint_tx_hash,
                minted_at,
            },
            metadata: HashMap::new(),
        });

        let UsdcRebalance::Bridged {
            direction,
            amount,
            burn_tx_hash: view_burn_tx,
            mint_tx_hash: view_mint_tx,
            initiated_at: view_initiated_at,
            minted_at: view_minted_at,
        } = view.live().unwrap()
        else {
            panic!("Expected Bridged state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*view_burn_tx, burn_tx_hash);
        assert_eq!(*view_mint_tx, mint_tx_hash);
        assert_eq!(*view_initiated_at, initiated_at);
        assert_eq!(*view_minted_at, minted_at);
    }

    #[test]
    fn test_view_tracks_bridging_failure() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();
        let confirmed_at = Utc::now();
        let burned_at = Utc::now();
        let failed_at = Utc::now();
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::WithdrawalConfirmed { confirmed_at },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 3,
            payload: UsdcRebalanceEvent::BridgingInitiated {
                burn_tx_hash,
                burned_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 4,
            payload: UsdcRebalanceEvent::BridgingFailed {
                burn_tx_hash: Some(burn_tx_hash),
                cctp_nonce: None, // nonce unknown when failing from Bridging state
                reason: "CCTP service unavailable".to_string(),
                failed_at,
            },
            metadata: HashMap::new(),
        });

        let UsdcRebalance::BridgingFailed {
            direction,
            amount,
            burn_tx_hash: view_burn_tx,
            cctp_nonce: view_nonce,
            reason,
            initiated_at: view_initiated_at,
            failed_at: view_failed_at,
        } = view.live().unwrap()
        else {
            panic!("Expected BridgingFailed state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*view_burn_tx, Some(burn_tx_hash));
        // nonce unknown when failing from Bridging state
        assert_eq!(*view_nonce, None);
        assert_eq!(reason, "CCTP service unavailable");
        assert_eq!(*view_initiated_at, initiated_at);
        assert_eq!(*view_failed_at, failed_at);
    }

    #[tokio::test]
    async fn test_initiate_deposit_with_alpaca_transfer() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: deposit_ref.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositInitiated event");
        };

        assert_eq!(*event_deposit_ref, deposit_ref);

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositInitiated {
            direction,
            amount,
            burn_tx_hash: state_burn_tx,
            mint_tx_hash: state_mint_tx,
            deposit_ref: state_deposit_ref,
            initiated_at: state_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositInitiated state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*state_burn_tx, burn_tx_hash);
        assert_eq!(*state_mint_tx, mint_tx_hash);
        assert_eq!(*state_deposit_ref, deposit_ref);
        assert_eq!(*state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_initiate_deposit_with_onchain_tx() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(500.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let deposit_tx_hash =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let deposit_ref = TransferRef::OnchainTx(deposit_tx_hash);

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: deposit_ref.clone(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositInitiated event");
        };

        assert_eq!(*event_deposit_ref, deposit_ref);

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositInitiated {
            direction,
            amount,
            deposit_ref: state_deposit_ref,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositInitiated state");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc(dec!(500.00)));
        assert_eq!(*state_deposit_ref, deposit_ref);
    }

    #[tokio::test]
    async fn test_cannot_deposit_before_bridging_complete() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::AlpacaId(deposit_transfer_id),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::BridgingNotCompleted
        ));
    }

    #[tokio::test]
    async fn test_confirm_deposit() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref,
            deposit_initiated_at: Utc::now(),
        });

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmDeposit, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::DepositConfirmed { .. }
        ));

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositConfirmed {
            direction,
            amount,
            burn_tx_hash: state_burn_tx,
            mint_tx_hash: state_mint_tx,
            initiated_at: state_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositConfirmed state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*state_burn_tx, burn_tx_hash);
        assert_eq!(*state_mint_tx, mint_tx_hash);
        assert_eq!(*state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_confirm_deposit_before_initiating() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmDeposit, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::DepositNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_fail_deposit_after_initiated() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(500.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at,
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let deposit_ref = TransferRef::AlpacaId(deposit_transfer_id);

        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: deposit_ref.clone(),
            deposit_initiated_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailDeposit {
                    reason: "Test failure reason".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositFailed {
            deposit_ref: event_deposit_ref,
            reason,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositFailed event");
        };

        assert_eq!(*event_deposit_ref, Some(deposit_ref.clone()));
        assert_eq!(reason, "Test failure reason");

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositFailed {
            direction,
            amount,
            deposit_ref: state_deposit_ref,
            reason: state_reason,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositFailed state");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc(dec!(500.00)));
        assert_eq!(*state_deposit_ref, Some(deposit_ref));
        assert_eq!(state_reason, "Test failure reason");
    }

    #[tokio::test]
    async fn test_deposit_failed_preserves_deposit_ref_when_available() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(transfer_id),
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash,
            burned_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01, 0x02],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash,
            minted_at: Utc::now(),
        });

        let onchain_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let deposit_ref = TransferRef::OnchainTx(onchain_tx);

        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: deposit_ref.clone(),
            deposit_initiated_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailDeposit {
                    reason: "Onchain deposit failed".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::DepositFailed {
            deposit_ref: event_deposit_ref,
            ..
        } = &events[0]
        else {
            panic!("Expected DepositFailed event");
        };

        assert_eq!(*event_deposit_ref, Some(deposit_ref.clone()));

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositFailed {
            deposit_ref: state_deposit_ref,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositFailed state");
        };

        assert_eq!(*state_deposit_ref, Some(deposit_ref));
    }

    #[tokio::test]
    async fn test_complete_alpaca_to_base_full_flow() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let initiated_at = Utc::now();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(10000.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let attestation = vec![0xAB, 0xCD, 0xEF];
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: attestation.clone(),
                    cctp_nonce: 12345,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let mint_tx_hash =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let deposit_tx =
            fixed_bytes!("0x2222222222222222222222222222222222222222222222222222222222222222");
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::OnchainTx(deposit_tx),
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmDeposit, &())
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositConfirmed {
            direction,
            amount,
            burn_tx_hash: final_burn_tx,
            mint_tx_hash: final_mint_tx,
            initiated_at: final_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositConfirmed state");
        };

        assert_eq!(*direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(*amount, Usdc(dec!(10000.00)));
        assert_eq!(*final_burn_tx, burn_tx_hash);
        assert_eq!(*final_mint_tx, mint_tx_hash);
        assert!(*final_initiated_at >= initiated_at);
    }

    #[tokio::test]
    async fn test_complete_base_to_alpaca_full_flow() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let withdrawal_tx =
            fixed_bytes!("0x3333333333333333333333333333333333333333333333333333333333333333");
        let initiated_at = Utc::now();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(5000.00)),
                    withdrawal: TransferRef::OnchainTx(withdrawal_tx),
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let burn_tx_hash =
            fixed_bytes!("0x4444444444444444444444444444444444444444444444444444444444444444");
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateBridging {
                    burn_tx: burn_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let attestation = vec![0x11, 0x22, 0x33, 0x44];
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation,
                    cctp_nonce: 67890,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let mint_tx_hash =
            fixed_bytes!("0x5555555555555555555555555555555555555555555555555555555555555555");
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::ConfirmBridging {
                    mint_tx: mint_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let deposit_transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateDeposit {
                    deposit: TransferRef::AlpacaId(deposit_transfer_id),
                },
                &(),
            )
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmDeposit, &())
            .await
            .unwrap();

        aggregate.apply(events.into_iter().next().unwrap());

        let UsdcRebalance::DepositConfirmed {
            direction,
            amount,
            burn_tx_hash: final_burn_tx,
            mint_tx_hash: final_mint_tx,
            initiated_at: final_initiated_at,
            ..
        } = aggregate.live().unwrap()
        else {
            panic!("Expected DepositConfirmed state");
        };

        assert_eq!(*direction, RebalanceDirection::BaseToAlpaca);
        assert_eq!(*amount, Usdc(dec!(5000.00)));
        assert_eq!(*final_burn_tx, burn_tx_hash);
        assert_eq!(*final_mint_tx, mint_tx_hash);
        assert!(*final_initiated_at >= initiated_at);
    }

    #[tokio::test]
    async fn test_withdrawal_failed_rejects_commands() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");

        let mut agg = Lifecycle::<UsdcRebalance, Never>::default();
        agg.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(100.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::WithdrawalFailed {
            reason: "Test failure".to_string(),
            failed_at: Utc::now(),
        });

        assert!(
            agg.handle(UsdcRebalanceCommand::ConfirmWithdrawal, &())
                .await
                .is_err()
        );
        assert!(
            agg.handle(UsdcRebalanceCommand::InitiateBridging { burn_tx }, &())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_bridging_failed_rejects_commands() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let mut agg = Lifecycle::<UsdcRebalance, Never>::default();
        agg.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(100.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgingFailed {
            burn_tx_hash: Some(burn_tx),
            cctp_nonce: None, // nonce unknown when failing from Bridging state
            reason: "Bridge failed".to_string(),
            failed_at: Utc::now(),
        });

        assert!(
            agg.handle(
                UsdcRebalanceCommand::ReceiveAttestation {
                    attestation: vec![0x01],
                    cctp_nonce: 12345,
                },
                &()
            )
            .await
            .is_err()
        );
        assert!(
            agg.handle(UsdcRebalanceCommand::ConfirmBridging { mint_tx }, &())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_deposit_failed_rejects_commands() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let mut agg = Lifecycle::<UsdcRebalance, Never>::default();
        agg.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(100.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash: mint_tx,
            minted_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::OnchainTx(mint_tx),
            deposit_initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::DepositFailed {
            deposit_ref: Some(TransferRef::OnchainTx(mint_tx)),
            reason: "Deposit failed".to_string(),
            failed_at: Utc::now(),
        });

        assert!(
            agg.handle(UsdcRebalanceCommand::ConfirmDeposit, &())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_deposit_confirmed_rejects_commands() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let mut agg = Lifecycle::<UsdcRebalance, Never>::default();
        agg.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(100.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash: mint_tx,
            minted_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::OnchainTx(mint_tx),
            deposit_initiated_at: Utc::now(),
        });
        agg.apply(UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at: Utc::now(),
        });

        assert!(
            agg.handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(100.00)),
                    withdrawal: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                },
                &()
            )
            .await
            .is_err()
        );
        assert!(
            agg.handle(UsdcRebalanceCommand::ConfirmDeposit, &())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_initiate_conversion_from_uninitialized() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    order_id,
                },
                &(),
            )
            .await
            .unwrap();

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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*event_order_id, order_id);
    }

    #[tokio::test]
    async fn test_cannot_initiate_conversion_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiateConversion {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(500.00)),
                    order_id: Uuid::new_v4(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::AlreadyInitiated
        ));
    }

    #[tokio::test]
    async fn test_confirm_conversion_from_converting_state() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at,
        });

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmConversion, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::ConversionConfirmed { .. }
        ));

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::ConversionComplete {
            direction,
            amount,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected ConversionComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_confirm_conversion_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmConversion, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::ConversionNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_confirm_conversion_twice() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::ConversionConfirmed {
            converted_at: Utc::now(),
        });

        let result = aggregate
            .handle(UsdcRebalanceCommand::ConfirmConversion, &())
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::ConversionAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_fail_conversion_from_converting_state() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let initiated_at = Utc::now();

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at,
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::FailConversion {
                    reason: "Order rejected".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::ConversionFailed { reason, .. } = &events[0] else {
            panic!("Expected ConversionFailed event");
        };
        assert_eq!(reason, "Order rejected");

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::ConversionFailed {
            direction,
            amount,
            order_id: state_order_id,
            reason: state_reason,
            initiated_at: state_initiated_at,
            ..
        }) = aggregate
        else {
            panic!("Expected ConversionFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(state_order_id, order_id);
        assert_eq!(state_reason, "Order rejected");
        assert_eq!(state_initiated_at, initiated_at);
    }

    #[tokio::test]
    async fn test_cannot_fail_conversion_before_initiating() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailConversion {
                    reason: "Test failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::ConversionNotInitiated
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_already_completed_conversion() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::ConversionConfirmed {
            converted_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::FailConversion {
                    reason: "Late failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::ConversionAlreadyCompleted
        ));
    }

    #[tokio::test]
    async fn test_initiate_withdrawal_after_conversion_complete() {
        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::ConversionConfirmed {
            converted_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::Initiate {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    withdrawal: TransferRef::AlpacaId(transfer_id),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], UsdcRebalanceEvent::Initiated { .. }));

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::Withdrawing {
            direction,
            amount,
            withdrawal_ref,
            ..
        }) = aggregate
        else {
            panic!("Expected Withdrawing state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(withdrawal_ref, TransferRef::AlpacaId(transfer_id));
    }

    #[test]
    fn test_view_tracks_conversion_initiation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let initiated_at = Utc::now();

        let event = UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at,
        };

        let envelope = EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        assert!(matches!(view, Lifecycle::Uninitialized));

        view.update(&envelope);

        let Lifecycle::Live(UsdcRebalance::Converting {
            direction,
            amount,
            order_id: view_order_id,
            initiated_at: view_initiated_at,
        }) = view
        else {
            panic!("Expected Live(Converting) variant");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_order_id, order_id);
        assert_eq!(view_initiated_at, initiated_at);
    }

    #[test]
    fn test_view_tracks_conversion_confirmation() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let initiated_at = Utc::now();
        let converted_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                order_id,
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::ConversionConfirmed { converted_at },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::ConversionComplete {
            direction,
            amount,
            initiated_at: view_initiated_at,
            converted_at: view_converted_at,
        }) = view
        else {
            panic!("Expected ConversionComplete state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_converted_at, converted_at);
    }

    #[test]
    fn test_view_tracks_conversion_failure() {
        let mut view = Lifecycle::<UsdcRebalance, Never>::default();
        let order_id = Uuid::new_v4();
        let initiated_at = Utc::now();
        let failed_at = Utc::now();

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 1,
            payload: UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                order_id,
                initiated_at,
            },
            metadata: HashMap::new(),
        });

        view.update(&EventEnvelope {
            aggregate_id: "rebalance-123".to_string(),
            sequence: 2,
            payload: UsdcRebalanceEvent::ConversionFailed {
                reason: "Order canceled".to_string(),
                failed_at,
            },
            metadata: HashMap::new(),
        });

        let Lifecycle::Live(UsdcRebalance::ConversionFailed {
            direction,
            amount,
            order_id: view_order_id,
            reason,
            initiated_at: view_initiated_at,
            failed_at: view_failed_at,
        }) = view
        else {
            panic!("Expected ConversionFailed state");
        };

        assert_eq!(direction, RebalanceDirection::AlpacaToBase);
        assert_eq!(amount, Usdc(dec!(1000.00)));
        assert_eq!(view_order_id, order_id);
        assert_eq!(reason, "Order canceled");
        assert_eq!(view_initiated_at, initiated_at);
        assert_eq!(view_failed_at, failed_at);
    }

    #[tokio::test]
    async fn test_initiate_post_deposit_conversion_from_deposit_confirmed() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = Uuid::new_v4();

        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::OnchainTx(burn_tx),
            initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash: mint_tx,
            minted_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            deposit_initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at: Utc::now(),
        });

        let events = aggregate
            .handle(
                UsdcRebalanceCommand::InitiatePostDepositConversion { order_id },
                &(),
            )
            .await
            .unwrap();

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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*event_order_id, order_id);
    }

    #[tokio::test]
    async fn test_cannot_initiate_post_deposit_conversion_for_alpaca_to_base() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");

        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash: mint_tx,
            minted_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::OnchainTx(mint_tx),
            deposit_initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: Uuid::new_v4(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::WrongDirectionForPostDepositConversion
        ));
    }

    #[tokio::test]
    async fn test_cannot_initiate_post_deposit_conversion_before_deposit_confirmed() {
        let aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        let result = aggregate
            .handle(
                UsdcRebalanceCommand::InitiatePostDepositConversion {
                    order_id: Uuid::new_v4(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result.unwrap_err(),
            UsdcRebalanceError::DepositNotConfirmed
        ));
    }

    #[tokio::test]
    async fn test_full_base_to_alpaca_flow_with_post_deposit_conversion() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = Uuid::new_v4();

        let mut aggregate = Lifecycle::<UsdcRebalance, Never>::default();

        aggregate.apply(UsdcRebalanceEvent::Initiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::OnchainTx(burn_tx),
            initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::WithdrawalConfirmed {
            confirmed_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgingInitiated {
            burn_tx_hash: burn_tx,
            burned_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: vec![0x01],
            cctp_nonce: 12345,
            attested_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::Bridged {
            mint_tx_hash: mint_tx,
            minted_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositInitiated {
            deposit_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            deposit_initiated_at: Utc::now(),
        });
        aggregate.apply(UsdcRebalanceEvent::DepositConfirmed {
            deposit_confirmed_at: Utc::now(),
        });

        aggregate.apply(UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::BaseToAlpaca,
            amount: Usdc(dec!(1000.00)),
            order_id,
            initiated_at: Utc::now(),
        });

        let events = aggregate
            .handle(UsdcRebalanceCommand::ConfirmConversion, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            UsdcRebalanceEvent::ConversionConfirmed { .. }
        ));

        aggregate.apply(events.into_iter().next().unwrap());

        let Lifecycle::Live(UsdcRebalance::ConversionComplete { direction, .. }) = aggregate else {
            panic!("Expected ConversionComplete state");
        };
        assert_eq!(direction, RebalanceDirection::BaseToAlpaca);
    }

    #[test]
    fn test_from_event_rejects_conversion_confirmed_as_init() {
        let event = UsdcRebalanceEvent::ConversionConfirmed {
            converted_at: Utc::now(),
        };

        let result = UsdcRebalance::from_event(&event);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[test]
    fn test_apply_transition_rejects_conversion_initiated_from_non_deposit_confirmed() {
        let current = UsdcRebalance::Withdrawing {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
            initiated_at: Utc::now(),
        };

        let event = UsdcRebalanceEvent::ConversionInitiated {
            direction: RebalanceDirection::AlpacaToBase,
            amount: Usdc(dec!(1000.00)),
            order_id: Uuid::new_v4(),
            initiated_at: Utc::now(),
        };

        let result = UsdcRebalance::apply_transition(&event, &current);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }
}
