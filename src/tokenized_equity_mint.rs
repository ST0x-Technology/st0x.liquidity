//! Aggregate modeling the lifecycle of minting tokenized
//! equities from underlying Alpaca shares.
//!
//! Tracks the workflow from requesting a mint through
//! Alpaca's tokenization API to receiving onchain tokens.
//!
//! # State Flow
//!
//! ```text
//! MintRequested -> MintAccepted -> TokensReceived -> TokensWrapped -> DepositedIntoRaindex
//!       |               |               |                 |
//!       v               v               v                 v
//!     Failed          Failed          Failed           Failed
//! ```
//!
//! - `MintRequested` can be rejected by Alpaca during `RequestMint`
//! - `MintAccepted` can fail via `Poll` (rejection/timeout/error)
//! - `DepositedIntoRaindex` and `Failed` are terminal states
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
//! 5. **Vault Deposit**: Wrapped tokens deposited into Raindex vault (terminal)
//!
//! # Error Handling
//!
//! The aggregate enforces strict state transitions:
//!
//! - Commands that don't match current state return appropriate errors
//! - Terminal states (DepositedIntoRaindex, Failed) reject all state-changing commands
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

/// Alpaca issuer request identifier returned when a tokenization request is accepted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

/// HTTP status code from API responses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct HttpStatusCode(pub(crate) u16);

/// Errors that can occur during tokenized equity mint operations.
///
/// These errors enforce state machine constraints and prevent
/// invalid transitions.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error)]
pub(crate) enum TokenizedEquityMintError {
    /// Command sent to a non-existent aggregate (must use RequestMint to initialize)
    #[error("Aggregate not initialized: use RequestMint to start a new mint")]
    NotInitialized,
    /// Attempted to request mint when already in progress
    #[error("Mint already in progress")]
    AlreadyInProgress,
    /// Attempted to deposit to vault before tokens were wrapped
    #[error("Cannot deposit to vault: tokens not wrapped")]
    TokensNotWrapped,
    /// Cannot proceed: mint has not been accepted yet
    #[error("Cannot proceed: mint not accepted")]
    NotAccepted,
    /// Attempted to wrap before tokens were received
    #[error("Cannot wrap: tokens not received for wrapping")]
    TokensNotReceivedForWrap,
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

/// Commands for the TokenizedEquityMint aggregate.
#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    /// Request tokenization from Alpaca and poll until tokens arrive or failure.
    ///
    /// Flow: MintRequested -> MintAccepted -> TokensReceived (success)
    ///                     or MintRejected (immediate failure)
    ///                     or MintAcceptanceFailed (failure after acceptance)
    /// Calls `request_mint()` on the tokenizer service.
    ///
    /// Emits MintRequested + MintAccepted (success)
    ///     or MintRequested + MintRejected (immediate rejection)
    RequestMint {
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    /// Calls `poll_mint_until_complete()` on the tokenizer service.
    ///
    /// Emits TokensReceived (success)
    ///     or MintAcceptanceFailed (rejection/timeout/error)
    Poll,
    WrapTokens {
        wrap_tx_hash: TxHash,
        wrapped_shares: U256,
    },
    DepositToVault {
        vault_deposit_tx_hash: TxHash,
    },
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
    /// Wrapping failed after tokens were received.
    WrappingFailed {
        symbol: Symbol,
        quantity: Decimal,
        failed_at: DateTime<Utc>,
    },

    /// Wrapped tokens deposited to Raindex vault.
    DepositedIntoRaindex {
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Raindex deposit failed after tokens were wrapped.
    /// Wrapped tokens remain in wallet, can be retried or manually recovered.
    RaindexDepositFailed {
        reason: String,
        failed_at: DateTime<Utc>,
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
            Self::WrappingFailed { .. } => "TokenizedEquityMintEvent::WrappingFailed".to_string(),
            Self::DepositedIntoRaindex { .. } => {
                "TokenizedEquityMintEvent::DepositedIntoRaindex".to_string()
            }
            Self::RaindexDepositFailed { .. } => {
                "TokenizedEquityMintEvent::RaindexDepositFailed".to_string()
            }
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
    DepositedIntoRaindex {
        symbol: Symbol,
        quantity: Decimal,
        /// Alpaca cross-system identifiers for auditing
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Token receipt transaction
        token_tx_hash: TxHash,
        /// Wrapping transaction
        wrap_tx_hash: TxHash,
        /// Vault deposit transaction
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },

    /// Mint operation failed (terminal state)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

/// Our tokenized equity tokens use 18 decimals.
pub(crate) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

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
            MintRequested { .. } | WrappingFailed { .. } => None,

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
            } => {
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
                } = entity
                else {
                    return Ok(None);
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
                    wrap_tx_hash: *wrap_tx_hash,
                    wrapped_shares: *wrapped_shares,
                    requested_at: *requested_at,
                    accepted_at: *accepted_at,
                    received_at: *received_at,
                    wrapped_at: *wrapped_at,
                })
            }

            DepositedIntoRaindex {
                vault_deposit_tx_hash,
                deposited_at,
            } => {
                let Self::TokensWrapped {
                    symbol,
                    quantity,
                    issuer_request_id,
                    tokenization_request_id,
                    tx_hash,
                    wrap_tx_hash,
                    ..
                } = entity
                else {
                    return Ok(None);
                };

                Some(Self::DepositedIntoRaindex {
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    token_tx_hash: *tx_hash,
                    wrap_tx_hash: *wrap_tx_hash,
                    vault_deposit_tx_hash: *vault_deposit_tx_hash,
                    deposited_at: *deposited_at,
                })
            }

            RaindexDepositFailed { reason, failed_at } => {
                let Self::TokensWrapped {
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
        })
    }

    async fn initialize(
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;

        let TokenizedEquityMintCommand::RequestMint {
            issuer_request_id,
            symbol,
            quantity,
            wallet,
        } = command
        else {
            return Err(TokenizedEquityMintError::NotInitialized);
        };

        let now = Utc::now();
        let mint_requested = MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at: now,
        };

        let alpaca_request = match services
            .tokenizer
            .request_mint(
                symbol,
                FractionalShares::new(quantity),
                wallet,
                issuer_request_id.clone(),
            )
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(%error, "Mint request failed");
                return Err(TokenizedEquityMintError::RequestFailed);
            }
        };

        if matches!(alpaca_request.status, TokenizationRequestStatus::Rejected) {
            return Ok(vec![
                mint_requested,
                MintRejected {
                    reason: "Rejected by Alpaca".to_string(),
                    rejected_at: now,
                },
            ]);
        }

        Ok(vec![
            mint_requested,
            MintAccepted {
                issuer_request_id,
                tokenization_request_id: alpaca_request.id,
                accepted_at: now,
            },
        ])
    }

    async fn transition(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        match command {
            TokenizedEquityMintCommand::RequestMint { .. } => {
                Err(TokenizedEquityMintError::AlreadyInProgress)
            }

            TokenizedEquityMintCommand::Poll => match self {
                Self::MintAccepted {
                    quantity,
                    tokenization_request_id,
                    ..
                } => {
                    let completed = match services
                        .tokenizer
                        .poll_mint_until_complete(tokenization_request_id)
                        .await
                    {
                        Ok(req) => req,
                        Err(error) => {
                            warn!(%error, "Polling failed");
                            return Ok(vec![MintAcceptanceFailed {
                                reason: format!("Polling failed: {error}"),
                                failed_at: Utc::now(),
                            }]);
                        }
                    };

                    match completed.status {
                        TokenizationRequestStatus::Completed => {
                            let tx_hash = completed
                                .tx_hash
                                .ok_or(TokenizedEquityMintError::MissingTxHash)?;
                            let shares_minted = decimal_to_u256_18_decimals(*quantity)?;

                            Ok(vec![TokensReceived {
                                tx_hash,
                                receipt_id: ReceiptId(U256::ZERO),
                                shares_minted,
                                received_at: Utc::now(),
                            }])
                        }
                        TokenizationRequestStatus::Rejected => Ok(vec![MintAcceptanceFailed {
                            reason: "Rejected by Alpaca after acceptance".to_string(),
                            failed_at: Utc::now(),
                        }]),
                        TokenizationRequestStatus::Pending => Ok(vec![MintAcceptanceFailed {
                            reason: "Unexpected Pending status after polling".to_string(),
                            failed_at: Utc::now(),
                        }]),
                    }
                }
                Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
                Self::TokensReceived { .. }
                | Self::TokensWrapped { .. }
                | Self::DepositedIntoRaindex { .. } => {
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
                Self::TokensWrapped { .. } | Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },

            TokenizedEquityMintCommand::DepositToVault {
                vault_deposit_tx_hash,
            } => match self {
                Self::TokensWrapped { .. } => Ok(vec![DepositedIntoRaindex {
                    vault_deposit_tx_hash,
                    deposited_at: Utc::now(),
                }]),
                Self::MintRequested { .. }
                | Self::MintAccepted { .. }
                | Self::TokensReceived { .. } => Err(TokenizedEquityMintError::TokensNotWrapped),
                Self::DepositedIntoRaindex { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rust_decimal_macros::dec;
    use st0x_event_sorcery::{AggregateError, LifecycleError, TestHarness, TestStore};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::Tokenizer;
    use crate::tokenization::mock::{MockMintPollOutcome, MockMintRequestOutcome, MockTokenizer};
    use crate::wrapper::mock::MockWrapper;

    fn mint_services(tokenizer: MockTokenizer) -> EquityTransferServices {
        EquityTransferServices {
            tokenizer: Arc::new(tokenizer),
            raindex: Arc::new(MockRaindex::new()),
            wrapper: Arc::new(MockWrapper::new()),
        }
    }

    fn mint_command() -> TokenizedEquityMintCommand {
        TokenizedEquityMintCommand::RequestMint {
            issuer_request_id: IssuerRequestId::new("ISS001"),
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(10),
            wallet: Address::ZERO,
        }
    }

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
        TokenizedEquityMintEvent::DepositedIntoRaindex {
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn initialize_emits_requested_and_accepted() {
        let events = TestHarness::<TokenizedEquityMint>::with(mint_services(MockTokenizer::new()))
            .given_no_previous_events()
            .when(mint_command())
            .await
            .events();

        assert_eq!(events.len(), 2);
        assert!(
            matches!(&events[0], TokenizedEquityMintEvent::MintRequested { symbol, .. } if symbol == &Symbol::new("AAPL").unwrap()),
            "Expected MintRequested, got: {:?}",
            events[0]
        );
        assert!(
            matches!(
                &events[1],
                TokenizedEquityMintEvent::MintAccepted { issuer_request_id, .. }
                    if issuer_request_id.0 == "ISS001"
            ),
            "Expected MintAccepted with ISS001, got: {:?}",
            events[1]
        );
    }

    #[tokio::test]
    async fn poll_after_acceptance_emits_tokens_received() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));

        let id = IssuerRequestId::new("ISS001");
        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensReceived { .. }),
            "Expected TokensReceived state, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_rejected_emits_acceptance_failed() {
        use crate::tokenization::mock::MockMintPollOutcome;

        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after rejected poll, got: {entity:?}"
        );
    }

    #[test]
    fn test_evolve_accepted_rejects_wrong_state() {
        let deposited = TokenizedEquityMint::DepositedIntoRaindex {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            token_tx_hash: TxHash::random(),
            wrap_tx_hash: TxHash::random(),
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS999".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK999".to_string()),
            accepted_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&deposited, &event).unwrap();
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

        let event = TokenizedEquityMintEvent::DepositedIntoRaindex {
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

    #[tokio::test]
    async fn request_mint_rejected_by_alpaca_emits_rejected() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::Rejected);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after rejection, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn request_mint_api_error_returns_error() {
        let tokenizer =
            MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::ApiError);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = IssuerRequestId::new("ISS001");

        let error = store.send(&id, mint_command()).await.unwrap_err();
        assert!(
            matches!(
                error,
                AggregateError::UserError(LifecycleError::Apply(
                    TokenizedEquityMintError::RequestFailed
                ))
            ),
            "Expected RequestFailed, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn poll_pending_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Pending);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after pending poll, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn poll_error_emits_acceptance_failed() {
        let tokenizer = MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::PollError);
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(tokenizer));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::Failed { .. }),
            "Expected Failed state after poll error, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn request_mint_passes_issuer_request_id_to_tokenizer() {
        let tokenizer: Arc<MockTokenizer> = Arc::new(MockTokenizer::new());
        let store = TestStore::<TokenizedEquityMint>::new(EquityTransferServices {
            tokenizer: Arc::clone(&tokenizer) as Arc<dyn Tokenizer>,
            raindex: Arc::new(MockRaindex::new()),
            wrapper: Arc::new(MockWrapper::new()),
        });
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();

        let recorded_id = tokenizer.last_issuer_request_id().unwrap();
        assert_eq!(recorded_id.0, "ISS001");
    }

    #[test]
    fn reject_mint_evolves_from_requested_to_failed() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(10),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintRejected {
            reason: "Insufficient balance".to_string(),
            rejected_at: Utc::now(),
        };

        let result = TokenizedEquityMint::evolve(&requested, &event)
            .unwrap()
            .unwrap();
        assert!(
            matches!(result, TokenizedEquityMint::Failed { ref reason, .. } if reason == "Insufficient balance"),
            "Expected Failed with reason, got: {result:?}"
        );
    }

    #[tokio::test]
    async fn wrap_tokens_is_pure_state_transition() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::random(),
                    wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::TokensWrapped { .. }),
            "Expected TokensWrapped, got: {entity:?}"
        );
    }

    #[tokio::test]
    async fn deposit_to_vault_is_pure_state_transition() {
        let store = TestStore::<TokenizedEquityMint>::new(mint_services(MockTokenizer::new()));
        let id = IssuerRequestId::new("ISS001");

        store.send(&id, mint_command()).await.unwrap();
        store
            .send(&id, TokenizedEquityMintCommand::Poll)
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash: TxHash::random(),
                    wrapped_shares: U256::from(10_000_000_000_000_000_000_u128),
                },
            )
            .await
            .unwrap();
        store
            .send(
                &id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::random(),
                },
            )
            .await
            .unwrap();

        let entity = store.load(&id).await.unwrap().unwrap();
        assert!(
            matches!(entity, TokenizedEquityMint::DepositedIntoRaindex { .. }),
            "Expected DepositedIntoRaindex, got: {entity:?}"
        );
    }

    #[test]
    fn evolve_full_happy_path_with_event_helpers() {
        let mut state: Option<TokenizedEquityMint> = None;

        let events = [
            mint_requested_event(),
            mint_accepted_event(),
            tokens_received_event(),
            tokens_wrapped_event(),
            vault_deposited_event(),
        ];

        for event in &events {
            state = state.as_ref().map_or_else(
                || TokenizedEquityMint::originate(event),
                |current| TokenizedEquityMint::evolve(current, event).unwrap(),
            );
        }

        assert!(
            matches!(
                state,
                Some(TokenizedEquityMint::DepositedIntoRaindex { .. })
            ),
            "Expected DepositedIntoRaindex after full lifecycle, got: {state:?}"
        );
    }
}
