//! Aggregate modeling the lifecycle of minting tokenized
//! equities from underlying Alpaca shares.
//!
//! Tracks the workflow from requesting a mint through
//! Alpaca's tokenization API to receiving onchain tokens.
//!
//! # State Flow
//!
//! ```text
//! MintRequested -> MintAccepted -> TokensReceived -> TokensWrapped -> VaultDeposited -> Completed
//!       |               |               |                |                 |
//!       v               v               v                v                 v
//!     Failed          Failed          Failed          Failed            Failed
//! ```
//!
//! - `MintRequested` can be rejected via `RejectMint`
//! - `MintAccepted` can fail via `FailAcceptance`
//! - `Completed` and `Failed` are terminal states
//!
//! # Alpaca API Integration
//!
//! The mint process integrates with Alpaca's tokenization API:
//!
//! 1. **Request**: System initiates mint request with symbol,
//!    quantity, and destination wallet
//! 2. **Acceptance**: Alpaca responds with `issuer_request_id` and
//!    `tokenization_request_id`
//! 3. **Transfer**: Alpaca executes onchain transfer, system detects
//!    transaction
//! 4. **Wrapping**: Tokens wrapped into ERC-4626 vault shares
//! 5. **Vault Deposit**: Wrapped tokens deposited into Raindex vault
//! 6. **Completion**: System verifies receipt and finalizes mint
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (Completed, Failed) reject all state-changing commands
//! - All state transitions are captured as events for complete audit trail

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use tracing::warn;

use st0x_event_sorcery::{DomainEvent, EventSourced, Table};
use st0x_execution::{FractionalShares, Symbol};

use crate::rebalancing::equity::EquityTransferServices;
use crate::tokenization::TokenizationRequestStatus;

/// Services required by the TokenizedEquityMint aggregate.
#[derive(Clone)]
pub(crate) struct MintServices {
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    pub(crate) raindex: Arc<dyn Raindex>,
}

/// Alpaca issuer request identifier returned when a tokenization request is accepted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssuerRequestId(pub(crate) String);

impl IssuerRequestId {
    pub(crate) fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

impl std::fmt::Display for IssuerRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for IssuerRequestId {
    type Err = std::convert::Infallible;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Ok(Self(value.to_string()))
    }
}

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

impl std::fmt::Display for TokenizationRequestId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Onchain receipt identifier (U256) for the token transfer transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReceiptId(pub(crate) U256);

/// Errors that can occur during tokenized equity mint operations.
///
/// These errors enforce state machine constraints and prevent
/// invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TokenizedEquityMintError {
    /// Attempted to request mint when already in progress
    #[error("Mint already in progress")]
    AlreadyInProgress,
    /// Attempted to acknowledge acceptance before requesting mint
    #[error("Cannot accept mint: not in requested state")]
    NotRequested,
    /// Attempted to receive tokens before mint was accepted
    #[error("Cannot receive tokens: mint not accepted")]
    NotAccepted,
    /// Attempted to wrap tokens before they were received
    #[error("Cannot wrap tokens: tokens not received")]
    TokensNotReceivedForWrap,
    /// Attempted to deposit to vault before tokens were wrapped
    #[error("Cannot deposit to vault: tokens not wrapped")]
    TokensNotWrapped,
    /// Attempted to finalize before vault deposit
    #[error("Cannot finalize: vault deposit not complete")]
    VaultDepositNotComplete,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Tokenizer API failed to submit the mint request
    #[error("Mint request failed")]
    RequestFailed,
    /// Completed mint response missing tx_hash
    #[error("Missing tx_hash in completed mint response")]
    MissingTxHash,
    /// Decimal overflow when scaling quantity to 18 decimals
    #[error("Decimal overflow when scaling {value} to 18 decimals")]
    DecimalScalingOverflow { value: Decimal },
    /// U256 conversion failed for a scaled decimal value
    #[error("Failed to convert scaled decimal {scaled_value} to U256")]
    U256ConversionFailed { scaled_value: String },
    /// Negative quantity is invalid for minting
    #[error("Negative quantity: {value}")]
    NegativeQuantity { value: Decimal },
    /// Input has more than 18 decimal places, conversion would lose precision
    #[error(
        "Precision loss: {value} has more than 18 decimal places \
         (scaled value {scaled} has fractional part)"
    )]
    PrecisionLoss { value: Decimal, scaled: Decimal },
    /// Vault lookup failed for the given symbol
    #[error("Vault lookup failed for {0}")]
    VaultLookupFailed(Symbol),
    /// Vault deposit transaction failed
    #[error("Vault deposit failed")]
    VaultDepositFailed,
}

#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    RequestMint {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    /// Alpaca rejected the mint request before acceptance.
    RejectMint {
        reason: String,
    },

    AcknowledgeAcceptance {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    },
    /// Mint failed after acceptance but before tokens were received.
    FailAcceptance {
        reason: String,
    },

    ReceiveTokens {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
    },

    /// Wrap unwrapped tokens into ERC-4626 vault shares.
    WrapTokens {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
    },

    /// Deposit wrapped tokens to Raindex vault.
    DepositToVault {
        vault_deposit_tx_hash: TxHash,
    },

    Finalize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMintEvent {
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    /// Alpaca rejected the mint request before acceptance.
    /// Shares remain in offchain available - no funds were moved.
    MintRejected {
        reason: String,
        rejected_at: DateTime<Utc>,
    },

    MintAccepted {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    /// Mint failed after acceptance but before tokens were received.
    /// Shares were moved to inflight, can be safely restored to offchain available.
    MintAcceptanceFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    TokensReceived {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    },

    /// Unwrapped tokens have been wrapped into ERC-4626 vault shares.
    TokensWrapped {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        wrapped_at: DateTime<Utc>,
    },

    /// Wrapped tokens deposited to Raindex vault.
    VaultDeposited {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },

    /// Vault deposit failed after tokens were wrapped.
    /// Wrapped tokens remain in wallet, can be retried or manually recovered.
    VaultDepositFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },

    MintCompleted {
        completed_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedEquityMintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::MintRequested { .. } => "TokenizedEquityMintEvent::MintRequested".to_string(),
            Self::MintRejected { .. } => "TokenizedEquityMintEvent::MintRejected".to_string(),
            Self::MintAccepted { .. } => "TokenizedEquityMintEvent::MintAccepted".to_string(),
            Self::MintAcceptanceFailed { .. } => {
                "TokenizedEquityMintEvent::MintAcceptanceFailed".to_string()
            }
            Self::TokensReceived { .. } => "TokenizedEquityMintEvent::TokensReceived".to_string(),
            Self::TokensWrapped { .. } => "TokenizedEquityMintEvent::TokensWrapped".to_string(),
            Self::VaultDeposited { .. } => "TokenizedEquityMintEvent::VaultDeposited".to_string(),
            Self::VaultDepositFailed { .. } => {
                "TokenizedEquityMintEvent::VaultDepositFailed".to_string()
            }
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Tokenized equity mint aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid
/// states unrepresentable. Each variant contains exactly the data
/// valid for that state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMint {
    /// Mint request initiated with symbol, quantity, and destination wallet
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },

    /// Alpaca API accepted the mint request and returned tracking identifiers
    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },

    /// Onchain token transfer detected with transaction details
    TokensReceived {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
    },

    /// Tokens have been wrapped into ERC-4626 vault shares
    TokensWrapped {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        wrapped_at: DateTime<Utc>,
    },

    /// Wrapped tokens deposited to Raindex vault
    VaultDeposited {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        token_tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        vault_deposit_tx_hash: TxHash,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
        received_at: DateTime<Utc>,
        wrapped_at: DateTime<Utc>,
        deposited_at: DateTime<Utc>,
    },

    /// Mint operation successfully completed (terminal state)
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        /// Alpaca cross-system identifiers for auditing
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Token receipt transaction from Alpaca
        token_tx_hash: TxHash,
        /// Vault deposit transaction to Raindex
        vault_deposit_tx_hash: TxHash,
        completed_at: DateTime<Utc>,
    },

    /// Mint operation failed with error reason (terminal state)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

/// Our tokenized equity tokens use 18 decimals.
const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

fn decimal_to_u256_18_decimals(value: Decimal) -> Result<U256, TokenizedEquityMintError> {
    if value.is_sign_negative() {
        return Err(TokenizedEquityMintError::NegativeQuantity { value });
    }

    let scale_factor = Decimal::from(10u64.pow(18));
    let scaled = value
        .checked_mul(scale_factor)
        .ok_or(TokenizedEquityMintError::DecimalScalingOverflow { value })?;

    if scaled.fract() != Decimal::ZERO {
        return Err(TokenizedEquityMintError::PrecisionLoss { value, scaled });
    }

    let repr = scaled.trunc().to_string();
    let Ok(amount) = U256::from_str_radix(&repr, 10) else {
        return Err(TokenizedEquityMintError::U256ConversionFailed { scaled_value: repr });
    };

    Ok(amount)
}

#[async_trait]
impl EventSourced for TokenizedEquityMint {
    type Id = IssuerRequestId;
    type Event = TokenizedEquityMintEvent;
    type Command = TokenizedEquityMintCommand;
    type Error = TokenizedEquityMintError;
    type Services = EquityTransferServices;

    const AGGREGATE_TYPE: &'static str = "TokenizedEquityMint";
    const PROJECTION: Option<Table> = None;
    const SCHEMA_VERSION: u64 = 1;

    fn originate(event: &Self::Event) -> Option<Self> {
        use TokenizedEquityMintEvent::*;
        match event {
            MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => Some(Self::MintRequested {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                requested_at: *requested_at,
            }),
            _ => None,
        }
    }

    fn evolve(entity: &Self, event: &Self::Event) -> Result<Option<Self>, Self::Error> {
        use TokenizedEquityMintEvent::*;

        Ok(match event {
            MintRequested { .. } => None,

            MintRejected {
                reason,
                rejected_at,
            } => {
                let Self::MintRequested {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason.to_string(),
                    requested_at: *requested_at,
                    failed_at: *rejected_at,
                })
            }

            MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => {
                let Self::MintRequested {
                    symbol,
                    quantity,
                    wallet,
                    requested_at,
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::MintAccepted {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                })
            }

            MintAcceptanceFailed { reason, failed_at } => {
                let Self::MintAccepted {
                    symbol,
                    quantity,
                    requested_at,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::Failed {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    reason: reason.to_string(),
                    requested_at: *requested_at,
                    failed_at: *failed_at,
                })
            }

            TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => {
                let Self::MintAccepted {
                    symbol,
                    quantity,
                    wallet,
                    issuer_request_id,
                    tokenization_request_id,
                    requested_at,
                    accepted_at,
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::TokensReceived {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    tx_hash: *tx_hash,
                    receipt_id: receipt_id.clone(),
                    shares_minted: *shares_minted,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                })
            }

            TokensWrapped {
                wrap_tx_hash,
                wrapped_shares,
                wrapped_at,
            } => entity.try_apply_tokens_wrapped(*wrap_tx_hash, *wrapped_shares, *wrapped_at),

            VaultDeposited {
                vault_deposit_tx_hash,
                deposited_at,
            } => entity.try_apply_vault_deposited(*vault_deposit_tx_hash, *deposited_at),

            VaultDepositFailed { reason, failed_at } => {
                entity.try_apply_vault_deposit_failed(reason, *failed_at)
            }

            MintCompleted { completed_at } => entity.try_apply_completed(*completed_at),
            }
        })
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedEquityMintCommand::RequestMint {
                symbol,
                quantity,
                wallet,
            } => Ok(vec![TokenizedEquityMintEvent::MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at: Utc::now(),
            }]),
            _ => Err(TokenizedEquityMintError::TokensNotReceived),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        match command {
            TokenizedEquityMintCommand::RequestMint { .. } => {
                Err(TokenizedEquityMintError::AlreadyInProgress)
            }

            TokenizedEquityMintCommand::RejectMint { reason } => match self {
                Self::MintRequested { .. } => Ok(vec![MintRejected {
                    reason,
                    rejected_at: Utc::now(),
                }]),
                Self::Completed { .. } | Self::VaultDeposited { .. } | Self::TokensWrapped { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::AcknowledgeAcceptance {
                issuer_request_id,
                tokenization_request_id,
            } => match self {
                Self::MintRequested { .. } => Ok(vec![MintAccepted {
                    issuer_request_id,
                    tokenization_request_id,
                    accepted_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::FailAcceptance { reason } => match self {
                Self::MintAccepted { .. } => Ok(vec![MintAcceptanceFailed {
                    reason,
                    failed_at: Utc::now(),
                }]),
                Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyInProgress),
            },

            TokenizedEquityMintCommand::ReceiveTokens {
                tx_hash,
                receipt_id,
                shares_minted,
            } => match self {
                Self::MintAccepted { .. } => Ok(vec![TokensReceived {
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    received_at: Utc::now(),
                }]),
                Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
                Self::Completed { .. } | Self::TokensReceived { .. } | Self::TokensWrapped { .. } | Self::VaultDeposited { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },

            TokenizedEquityMintCommand::WrapTokens {
                wrap_tx_hash,
                wrapped_shares,
            } => match self {
                Self::TokensReceived { .. } => Ok(vec![TokensWrapped {
                    wrap_tx_hash,
                    wrapped_shares,
                    wrapped_at: Utc::now(),
                }]),
                Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Err(TokenizedEquityMintError::TokensNotReceivedForWrap)
                }
                Self::Completed { .. } | Self::TokensWrapped { .. } | Self::VaultDeposited { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },

            TokenizedEquityMintCommand::DepositToVault {
                vault_deposit_tx_hash,
            } => match self {
                Self::TokensWrapped { .. } => Ok(vec![VaultDeposited {
                    vault_deposit_tx_hash,
                    deposited_at: Utc::now(),
                }]),
                Self::MintRequested { .. } | Self::MintAccepted { .. } | Self::TokensReceived { .. } => {
                    Err(TokenizedEquityMintError::TokensNotWrapped)
                }
                Self::Completed { .. } | Self::VaultDeposited { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },

            TokenizedEquityMintCommand::Finalize => match self {
                Self::VaultDeposited { .. } => Ok(vec![MintCompleted {
                    completed_at: Utc::now(),
                }]),
                Self::MintRequested { .. } | Self::MintAccepted { .. } | Self::TokensReceived { .. } | Self::TokensWrapped { .. } => {
                    Err(TokenizedEquityMintError::VaultDepositNotComplete)
                }
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
        }
    }
}

impl TokenizedEquityMint {
    fn try_apply_accepted(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::MintRequested {
            symbol,
            quantity,
            wallet,
            requested_at,
        } = self
        else {
            return None;
        };

        Some(Self::MintAccepted {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            requested_at: *requested_at,
            accepted_at,
        })
    }

    fn try_apply_tokens_received(
        &self,
        tx_hash: TxHash,
        receipt_id: &ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::MintAccepted {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            requested_at,
            accepted_at,
        } = self
        else {
            return None;
        };

        Some(Self::TokensReceived {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted,
            requested_at: *requested_at,
            accepted_at: *accepted_at,
            received_at,
        })
    }

    fn try_apply_tokens_wrapped(
        &self,
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
        wrapped_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::TokensReceived {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            tx_hash,
            receipt_id,
            shares_minted,
            requested_at,
            accepted_at,
            received_at,
        } = self
        else {
            return None;
        };

        Some(Self::TokensWrapped {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            tx_hash: *tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted: *shares_minted,
            wrap_tx_hash,
            wrapped_shares,
            requested_at: *requested_at,
            accepted_at: *accepted_at,
            received_at: *received_at,
            wrapped_at,
        })
    }

    fn try_apply_vault_deposited(
        &self,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::TokensWrapped {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            tx_hash,
            receipt_id,
            shares_minted,
            wrap_tx_hash,
            wrapped_shares,
            requested_at,
            accepted_at,
            received_at,
            wrapped_at,
        } = self
        else {
            return None;
        };

        Some(Self::VaultDeposited {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            token_tx_hash: *tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted: *shares_minted,
            wrap_tx_hash: *wrap_tx_hash,
            wrapped_shares: *wrapped_shares,
            vault_deposit_tx_hash,
            requested_at: *requested_at,
            accepted_at: *accepted_at,
            received_at: *received_at,
            wrapped_at: *wrapped_at,
            deposited_at,
        })
    }

    fn try_apply_vault_deposit_failed(
        &self,
        reason: &str,
        failed_at: DateTime<Utc>,
    ) -> Option<Self> {
        let Self::TokensWrapped {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }

    fn try_apply_completed(&self, completed_at: DateTime<Utc>) -> Option<Self> {
        let Self::VaultDeposited {
            symbol,
            quantity,
            issuer_request_id,
            tokenization_request_id,
            token_tx_hash,
            vault_deposit_tx_hash,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Completed {
            symbol: symbol.clone(),
            quantity: *quantity,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            token_tx_hash: *token_tx_hash,
            vault_deposit_tx_hash: *vault_deposit_tx_hash,
            completed_at,
        })
    }

    fn try_apply_rejected(&self, reason: &str, rejected_at: DateTime<Utc>) -> Option<Self> {
        let Self::MintRequested {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at: rejected_at,
        })
    }

    fn try_apply_acceptance_failed(&self, reason: &str, failed_at: DateTime<Utc>) -> Option<Self> {
        let Self::MintAccepted {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return None;
        };

        Some(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;

    use super::*;

    fn mint_requested_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        }
    }

    fn mint_accepted_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        }
    }

    fn tokens_received_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        }
    }

    fn tokens_wrapped_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(100_000_000_000_000_000_000_u128),
            wrapped_at: Utc::now(),
        }
    }

    fn vault_deposited_event() -> TokenizedEquityMintEvent {
        TokenizedEquityMintEvent::VaultDeposited {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        }
    }

    #[test]
    fn test_evolve_accepted_rejects_wrong_state() {
        let completed = TokenizedEquityMint::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            token_tx_hash: TxHash::random(),
            vault_deposit_tx_hash: TxHash::random(),
            completed_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS999".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK999".to_string()),
            accepted_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&completed, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_tokens_received_rejects_wrong_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::TokensReceived {
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_completed_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintCompleted {
            completed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejected_rejects_non_requested_states() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRejected {
            reason: "Should not apply".to_string(),
            rejected_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_acceptance_failed_rejects_non_accepted_states() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_rejects_mint_requested_on_existing_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRequested {
            symbol: Symbol::new("GOOG").unwrap(),
            quantity: dec!(50.0),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_evolve_vault_deposited_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::VaultDeposited {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn decimal_to_u256_18_decimals_rejects_negative() {
        let error = decimal_to_u256_18_decimals(dec!(-5)).unwrap_err();
        assert!(
            matches!(error, TokenizedEquityMintError::NegativeQuantity { .. }),
            "Expected NegativeQuantity, got: {error:?}"
        );
    }

    #[test]
    fn decimal_to_u256_18_decimals_converts_correctly() {
        let result = decimal_to_u256_18_decimals(dec!(3)).unwrap();
        assert_eq!(result, U256::from(3_000_000_000_000_000_000_u128));
    }

    #[test]
    fn decimal_to_u256_18_decimals_zero_returns_zero() {
        let result = decimal_to_u256_18_decimals(dec!(0)).unwrap();
        assert_eq!(result, U256::ZERO);
    }

    #[test]
    fn decimal_to_u256_18_decimals_rejects_19_decimal_places() {
        let value = Decimal::from_str("1.1234567890123456789").unwrap();
        let error = decimal_to_u256_18_decimals(value).unwrap_err();
        assert!(
            matches!(error, TokenizedEquityMintError::PrecisionLoss { .. }),
            "Expected PrecisionLoss, got: {error:?}"
        );
    }

    #[test]
    fn decimal_to_u256_18_decimals_accepts_exactly_18_decimal_places() {
        let value = Decimal::from_str("1.123456789012345678").unwrap();
        let result = decimal_to_u256_18_decimals(value).unwrap();
        assert_eq!(result, U256::from(1_123_456_789_012_345_678_u128));
    }

    #[tokio::test]
    async fn mint_uses_command_issuer_request_id() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert!(
            matches!(
                &events[1],
                TokenizedEquityMintEvent::MintAccepted { issuer_request_id, .. }
                    if issuer_request_id.0 == "ISS001"
            ),
            "Expected issuer_request_id from command, got: {:?}",
            events[1]
        );
    }

    #[test]
    fn test_evolve_tokens_wrapped_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::TokensWrapped {
            wrap_tx_hash: TxHash::random(),
            wrapped_shares: U256::from(100_000_000_000_000_000_000_u128),
            wrapped_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&accepted, &event).unwrap();
        assert_eq!(result, None);
    }
}
