//! Aggregate modeling the lifecycle of cross-chain USDC
//! rebalancing between Alpaca and Base via CCTP.
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
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use uuid::Uuid;

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};

use crate::alpaca_wallet::AlpacaTransferId;
use crate::threshold::Usdc;

/// Unique identifier for a USDC rebalance operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
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
        order_id: Uuid,
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
    InitiatePostDepositConversion { order_id: Uuid, amount: Usdc },
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
        order_id: Uuid,
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
        cctp_nonce: Option<u64>,
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
impl EventSourced for UsdcRebalance {
    type Id = UsdcRebalanceId;
    type Event = UsdcRebalanceEvent;
    type Command = UsdcRebalanceCommand;
    type Error = UsdcRebalanceError;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "UsdcRebalance";
    const PROJECTION: Option<Table> = None;
    const SCHEMA_VERSION: u64 = 1;

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
                order_id: *order_id,
                initiated_at: *initiated_at,
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
                    initiated_at,
                },
                Self::DepositConfirmed { .. },
            ) => Self::Converting {
                direction: direction.clone(),
                amount: *amount,
                order_id: *order_id,
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
                order_id: *order_id,
                reason: reason.to_string(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
            },

            (
                Initiated {
                    withdrawal_ref,
                    initiated_at,
                    ..
                },
                Self::ConversionComplete {
                    direction,
                    filled_amount,
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
                reason: reason.to_string(),
                initiated_at: *initiated_at,
                failed_at: *failed_at,
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
                    attested_at,
                },
                Self::Bridging {
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
                initiated_at: *initiated_at,
                attested_at: *attested_at,
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
                Self::Bridging {
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
                reason: reason.to_string(),
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
                    amount,
                    burn_tx_hash,
                    mint_tx_hash,
                    initiated_at,
                    ..
                },
            ) => Self::DepositInitiated {
                direction: direction.clone(),
                amount: *amount,
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
                reason: reason.to_string(),
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

            InitiateBridging { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),

            ReceiveAttestation { .. } | FailBridging { .. } => {
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
            Initiate {
                direction,
                amount,
                withdrawal,
            } => self.transition_initiate_withdrawal(direction, amount, withdrawal),
            ConfirmWithdrawal => self.transition_confirm_withdrawal(),
            FailWithdrawal { reason } => self.transition_fail_withdrawal(reason),
            InitiateBridging { burn_tx } => self.transition_initiate_bridging(burn_tx),
            ReceiveAttestation {
                attestation,
                cctp_nonce,
            } => self.transition_receive_attestation(attestation, cctp_nonce),
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
        order_id: Uuid,
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

    fn transition_initiate_withdrawal(
        &self,
        direction: RebalanceDirection,
        amount: Usdc,
        withdrawal: TransferRef,
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
                command: "Initiate".to_string(),
                state: "ConversionComplete with different \
                        direction"
                    .to_string(),
            });
        }
        if amount != *conv_filled_amount {
            return Err(UsdcRebalanceError::InvalidCommand {
                command: "Initiate".to_string(),
                state: format!(
                    "ConversionComplete with amount \
                     mismatch: expected {}, got {}",
                    conv_filled_amount.0, amount.0
                ),
            });
        }
        Ok(vec![Initiated {
            direction,
            amount: *conv_filled_amount,
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
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
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
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
            | Self::Attested { .. }
            | Self::Bridged { .. }
            | Self::BridgingFailed { .. }
            | Self::DepositInitiated { .. }
            | Self::DepositConfirmed { .. }
            | Self::DepositFailed { .. } => Err(UsdcRebalanceError::WithdrawalAlreadyCompleted),
            _ => Err(UsdcRebalanceError::WithdrawalNotInitiated),
        }
    }

    fn transition_initiate_bridging(
        &self,
        burn_tx: TxHash,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::WithdrawalNotConfirmed),
            Self::WithdrawalComplete { .. } => Ok(vec![BridgingInitiated {
                burn_tx_hash: burn_tx,
                burned_at: Utc::now(),
            }]),
            Self::Bridging { .. }
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
        cctp_nonce: u64,
    ) -> Result<Vec<UsdcRebalanceEvent>, UsdcRebalanceError> {
        use UsdcRebalanceEvent::*;
        match self {
            Self::Converting { .. }
            | Self::ConversionComplete { .. }
            | Self::ConversionFailed { .. }
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            Self::Bridging { .. } => Ok(vec![BridgeAttestationReceived {
                attestation,
                cctp_nonce,
                attested_at: Utc::now(),
            }]),
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
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. } => Err(UsdcRebalanceError::AttestationNotReceived),
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
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. } => Err(UsdcRebalanceError::BridgingNotInitiated),
            Self::Bridging { burn_tx_hash, .. } => Ok(vec![BridgingFailed {
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
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
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
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
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
            | Self::Withdrawing { .. }
            | Self::WithdrawalComplete { .. }
            | Self::WithdrawalFailed { .. }
            | Self::Bridging { .. }
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
    use rust_decimal_macros::dec;
    use uuid::Uuid;

    use st0x_event_sorcery::{LifecycleError, TestHarness, replay};

    use super::*;

    #[tokio::test]
    async fn test_initiate_alpaca_to_base() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with(())
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
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
                amount: Usdc(dec!(500.50)),
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
        assert_eq!(*amount, Usdc(dec!(500.50)));
        assert_eq!(*withdrawal_ref, TransferRef::OnchainTx(tx_hash));
    }

    #[tokio::test]
    async fn test_cannot_initiate_twice() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::Initiate {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc(dec!(500.00)),
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
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::BaseToAlpaca,
                amount: Usdc(dec!(500.00)),
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
    async fn test_confirm_withdrawal() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::Initiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
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
                cctp_nonce: 12345,
            })
            .await
            .events();

        assert_eq!(events.len(), 1);
        let UsdcRebalanceEvent::BridgeAttestationReceived {
            attestation: event_attestation,
            ..
        } = &events[0]
        else {
            panic!("Expected BridgeAttestationReceived event");
        };

        assert_eq!(*event_attestation, attestation);
    }

    #[tokio::test]
    async fn test_cannot_receive_attestation_before_bridging() {
        let error = TestHarness::<UsdcRebalance>::with(())
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: 12345,
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
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: 12345,
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
                    amount: Usdc(dec!(1000.00)),
                    withdrawal_ref: TransferRef::AlpacaId(transfer_id),
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::WithdrawalConfirmed {
                    confirmed_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x01, 0x02],
                cctp_nonce: 12345,
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
                    amount: Usdc(dec!(1000.00)),
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
                cctp_nonce: 12345,
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ReceiveAttestation {
                attestation: vec![0x03, 0x04],
                cctp_nonce: 99999,
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx: mint_tx_hash,
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
            })
            .await
            .then_expect_error();

        assert!(matches!(
            error,
            LifecycleError::Apply(UsdcRebalanceError::AttestationNotReceived)
        ));
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
                    amount: Usdc(dec!(1000.00)),
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

    #[tokio::test]
    async fn test_fail_bridging_after_attestation_received() {
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let burn_tx_hash =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let cctp_nonce = 12345u64;

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
                    minted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmBridging {
                mint_tx: fixed_bytes!(
                    "0x2222222222222222222222222222222222222222222222222222222222222222"
                ),
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(500.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(500.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(10000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(5000.00)),
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
                    cctp_nonce: 67890,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(100.00)),
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
                    amount: Usdc(dec!(100.00)),
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
                    amount: Usdc(dec!(100.00)),
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
                cctp_nonce: 12345,
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
                    amount: Usdc(dec!(100.00)),
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
                amount_received: Usdc(dec!(99.99)),
                fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(100.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(100.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                amount: Usdc(dec!(100.00)),
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
                    amount: Usdc(dec!(100.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
        let order_id = Uuid::new_v4();

        let events = TestHarness::<UsdcRebalance>::with(())
            .given_no_previous_events()
            .when(UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                order_id,
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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
        assert_eq!(*event_order_id, order_id);
    }

    #[tokio::test]
    async fn test_cannot_initiate_conversion_twice() {
        let order_id = Uuid::new_v4();

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                order_id,
                initiated_at: Utc::now(),
            }])
            .when(UsdcRebalanceCommand::InitiateConversion {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(500.00)),
                order_id: Uuid::new_v4(),
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
        let order_id = Uuid::new_v4();
        let filled_amount = Usdc(dec!(998));

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
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
                filled_amount: Usdc(dec!(998)),
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
        let order_id = Uuid::new_v4();

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount: Usdc(dec!(998)),
                    converted_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: Usdc(dec!(998)),
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
        let order_id = Uuid::new_v4();

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
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
        let order_id = Uuid::new_v4();

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
                UsdcRebalanceEvent::ConversionConfirmed {
                    direction: RebalanceDirection::AlpacaToBase,
                    filled_amount: Usdc(dec!(998)),
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
        let order_id = Uuid::new_v4();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let filled_amount = Usdc(dec!(998));

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
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
        let order_id = Uuid::new_v4();
        let transfer_id = AlpacaTransferId::from(Uuid::new_v4());
        let filled_amount = Usdc(dec!(998));

        let error = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::ConversionInitiated {
                    direction: RebalanceDirection::AlpacaToBase,
                    amount: Usdc(dec!(1000.00)),
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
                amount: Usdc(dec!(999.00)),
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
        let order_id = Uuid::new_v4();

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                order_id,
                amount: Usdc(dec!(1000.00)),
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
        assert_eq!(*amount, Usdc(dec!(1000.00)));
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                order_id: Uuid::new_v4(),
                amount: Usdc(dec!(1000.00)),
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
                order_id: Uuid::new_v4(),
                amount: Usdc(dec!(1000.00)),
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
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                order_id: Uuid::new_v4(),
                amount: Usdc(dec!(500.00)),
            })
            .await
            .then_expect_error();

        assert!(
            matches!(
                &error,
                LifecycleError::Apply(UsdcRebalanceError::ConversionAmountMismatch { expected, provided })
                    if *expected == Usdc(dec!(1000.00)) && *provided == Usdc(dec!(500.00))
            ),
            "Expected ConversionAmountMismatch with expected=1000 and provided=500, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn test_full_base_to_alpaca_flow_with_post_deposit_conversion() {
        let burn_tx =
            fixed_bytes!("0x0000000000000000000000000000000000000000000000000000000000000001");
        let mint_tx =
            fixed_bytes!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let order_id = Uuid::new_v4();

        let events = TestHarness::<UsdcRebalance>::with(())
            .given(vec![
                UsdcRebalanceEvent::Initiated {
                    direction: RebalanceDirection::BaseToAlpaca,
                    amount: Usdc(dec!(1000.00)),
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
                    cctp_nonce: 12345,
                    attested_at: Utc::now(),
                },
                UsdcRebalanceEvent::Bridged {
                    mint_tx_hash: mint_tx,
                    amount_received: Usdc(dec!(99.99)),
                    fee_collected: Usdc(dec!(0.01)),
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
                    amount: Usdc(dec!(1000.00)),
                    order_id,
                    initiated_at: Utc::now(),
                },
            ])
            .when(UsdcRebalanceCommand::ConfirmConversion {
                filled_amount: Usdc(dec!(998)),
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
    fn conversion_confirmed_on_uninitialized_produces_failed_state() {
        let error = replay::<UsdcRebalance>(vec![UsdcRebalanceEvent::ConversionConfirmed {
            direction: RebalanceDirection::BaseToAlpaca,
            filled_amount: Usdc(dec!(998)),
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
                amount: Usdc(dec!(1000.00)),
                withdrawal_ref: TransferRef::AlpacaId(AlpacaTransferId::from(Uuid::new_v4())),
                initiated_at: Utc::now(),
            },
            UsdcRebalanceEvent::ConversionInitiated {
                direction: RebalanceDirection::AlpacaToBase,
                amount: Usdc(dec!(1000.00)),
                order_id: Uuid::new_v4(),
                initiated_at: Utc::now(),
            },
        ])
        .unwrap_err();

        assert!(matches!(error, LifecycleError::UnexpectedEvent { .. }));
    }
}
