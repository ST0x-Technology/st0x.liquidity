//! Tokenized Equity Mint aggregate for Alpaca-to-Raindex inventory transfer.
//!
//! Moves equity inventory from Alpaca (brokerage) to Raindex (onchain orderbook)
//! by tokenizing shares and depositing them to a vault for liquidity provision.
//! This is one direction of cross-venue equity rebalancing (the reverse is
//! `EquityRedemption`).
//!
//! # State Flow
//!
//! ```text
//! MintRequested -> MintAccepted -> TokensReceived -> DepositedIntoRaindex -> Completed
//!       |               |               |
//!       v               v               v
//!     Failed          Failed          Failed
//! ```
//!
//! Terminal states (`Completed`, `Failed`) capture audit-critical fields not
//! available from earlier events (tx hashes, tracking IDs, timestamps).

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::persist::PersistedEventStore;
use cqrs_es::{Aggregate, DomainEvent};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlite_es::SqliteEventRepository;
use st0x_execution::{FractionalShares, SharesConversionError, Symbol};
use std::sync::Arc;

use crate::equity_redemption::TOKENIZED_EQUITY_DECIMALS;
use crate::lifecycle::{Lifecycle, LifecycleError, Never};
use crate::onchain::raindex::{Raindex, RaindexError};
use crate::tokenization::{TokenizationRequestStatus, Tokenizer, TokenizerError};

/// SQLite-backed event store for TokenizedEquityMint aggregates.
pub(crate) type MintEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<TokenizedEquityMint, Never>>;

/// Services required by the TokenizedEquityMint aggregate.
///
/// Combines `Tokenizer` (for Alpaca mint operations) and `Raindex` (for depositing
/// to Rain OrderBook) traits.
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
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, thiserror::Error)]
pub(crate) enum TokenizedEquityMintError {
    /// Attempted to request mint when already in progress
    #[error("Mint already in progress")]
    AlreadyInProgress,
    /// Attempted to deposit to vault before tokens were received
    #[error("Cannot deposit to vault: tokens not received")]
    TokensNotReceived,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Lifecycle state error
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
    /// Tokenizer service error
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    /// Vault service error
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    /// Missing tx_hash in completed tokenization request
    #[error("Completed tokenization request missing tx_hash")]
    MissingTxHash,
    /// Shares conversion error (negative, overflow, underflow)
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
}

/// Commands for the TokenizedEquityMint aggregate.
#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    /// Request tokenization from Alpaca and poll until tokens arrive or failure.
    ///
    /// Flow: MintRequested -> MintAccepted -> TokensReceived (success)
    ///                     or MintRejected (immediate failure)
    ///                     or MintAcceptanceFailed (failure after acceptance)
    Mint {
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    /// Deposit received tokens into Raindex vault.
    ///
    /// Flow: DepositedIntoRaindex -> Completed (success)
    ///    or RaindexDepositFailed (failure, terminal)
    Deposit,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
        symbol: Symbol,
        quantity: Decimal,
        status_code: Option<HttpStatusCode>,
        rejected_at: DateTime<Utc>,
    },

    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    /// Mint failed after acceptance but before tokens were received.
    /// Shares were moved to inflight, can be safely restored to offchain available.
    MintAcceptanceFailed {
        symbol: Symbol,
        quantity: Decimal,
        last_status: TokenizationRequestStatus,
        failed_at: DateTime<Utc>,
    },

    TokensReceived {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    },

    /// Tokens deposited from wallet to Raindex vault.
    DepositedIntoRaindex {
        symbol: Symbol,
        quantity: Decimal,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Vault deposit failed after tokens were received.
    /// Tokens remain in wallet, can be retried or manually recovered.
    RaindexDepositFailed {
        symbol: Symbol,
        quantity: Decimal,
        failed_tx_hash: Option<TxHash>,
        failed_at: DateTime<Utc>,
    },

    Completed {
        symbol: Symbol,
        quantity: Decimal,
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
            Self::DepositedIntoRaindex { .. } => {
                "TokenizedEquityMintEvent::DepositedIntoRaindex".to_string()
            }
            Self::RaindexDepositFailed { .. } => {
                "TokenizedEquityMintEvent::RaindexDepositFailed".to_string()
            }
            Self::Completed { .. } => "TokenizedEquityMintEvent::Completed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}

/// Tokenized equity mint aggregate state machine.
///
/// Uses the typestate pattern via enum variants to make invalid states unrepresentable.
/// Each variant contains exactly the data valid for that state.
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

    /// Tokens deposited from wallet to Raindex vault
    DepositedIntoRaindex {
        symbol: Symbol,
        quantity: Decimal,
        /// Alpaca cross-system identifiers for auditing
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        /// Token receipt transaction
        token_tx_hash: TxHash,
        /// Vault deposit transaction
        vault_deposit_tx_hash: TxHash,
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

    /// Mint operation failed (terminal state)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl Aggregate for Lifecycle<TokenizedEquityMint, Never> {
    type Command = TokenizedEquityMintCommand;
    type Event = TokenizedEquityMintEvent;
    type Error = TokenizedEquityMintError;
    type Services = MintServices;

    fn aggregate_type() -> String {
        "TokenizedEquityMint".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        *self = self
            .clone()
            .transition(&event, TokenizedEquityMint::apply_transition)
            .or_initialize(&event, TokenizedEquityMint::from_event);
    }

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedEquityMintCommand::Mint {
                issuer_request_id,
                symbol,
                quantity,
                wallet,
            } => {
                self.handle_mint(services, issuer_request_id, symbol, quantity, wallet)
                    .await
            }
            TokenizedEquityMintCommand::Deposit => self.handle_deposit(services).await,
        }
    }
}

/// Represents the outcome of the mint workflow after the initial request.
enum MintWorkflowOutcome {
    /// Tokenizer rejected the mint request immediately.
    Rejected { status_code: Option<HttpStatusCode> },
    /// Tokenizer accepted but polling failed or returned non-completed status.
    AcceptanceFailed {
        accepted_event: TokenizedEquityMintEvent,
        last_status: TokenizationRequestStatus,
    },
    /// Tokenizer completed successfully and tokens were received.
    TokensReceived {
        accepted_event: TokenizedEquityMintEvent,
        tx_hash: TxHash,
        shares_minted: U256,
    },
}

impl Lifecycle<TokenizedEquityMint, Never> {
    /// Handles the Mint command: requests tokenization from Alpaca and polls until completion.
    ///
    /// Flow: MintRequested -> MintAccepted -> TokensReceived (success)
    ///                     or MintRejected (immediate failure)
    ///                     or MintAcceptanceFailed (failure after acceptance)
    async fn handle_mint(
        &self,
        services: &MintServices,
        issuer_request_id: IssuerRequestId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        self.validate_mint_preconditions()?;

        let fractional_quantity = FractionalShares::new(quantity);
        let outcome = Self::execute_mint_workflow(
            services,
            &issuer_request_id,
            &symbol,
            fractional_quantity,
            wallet,
        )
        .await?;

        let requested = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at: Utc::now(),
        };

        Ok(Self::outcome_to_events(
            outcome,
            requested,
            symbol,
            quantity,
            fractional_quantity,
        ))
    }

    fn validate_mint_preconditions(&self) -> Result<(), TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Ok(()),
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Ok(TokenizedEquityMint::Completed { .. }) => {
                Err(TokenizedEquityMintError::AlreadyCompleted)
            }
            Ok(_) => Err(TokenizedEquityMintError::AlreadyInProgress),
            Err(e) => Err(e.into()),
        }
    }

    async fn execute_mint_workflow(
        services: &MintServices,
        issuer_request_id: &IssuerRequestId,
        symbol: &Symbol,
        fractional_quantity: FractionalShares,
        wallet: Address,
    ) -> Result<MintWorkflowOutcome, TokenizedEquityMintError> {
        let alpaca_request = match services
            .tokenizer
            .request_mint(
                symbol.clone(),
                fractional_quantity,
                wallet,
                issuer_request_id.clone(),
            )
            .await
        {
            Ok(req) => req,
            Err(e) => {
                let status_code = match &e {
                    TokenizerError::Alpaca(alpaca_err) => {
                        alpaca_err.status_code().map(|c| HttpStatusCode(c.as_u16()))
                    }
                };
                return Ok(MintWorkflowOutcome::Rejected { status_code });
            }
        };

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: fractional_quantity.inner(),
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: alpaca_request.id.clone(),
            accepted_at: Utc::now(),
        };

        let Ok(completed_request) = services
            .tokenizer
            .poll_mint_until_complete(&alpaca_request.id)
            .await
        else {
            return Ok(MintWorkflowOutcome::AcceptanceFailed {
                accepted_event,
                last_status: TokenizationRequestStatus::Pending,
            });
        };

        match completed_request.status {
            TokenizationRequestStatus::Completed => {
                let tx_hash = completed_request
                    .tx_hash
                    .ok_or(TokenizedEquityMintError::MissingTxHash)?;
                let shares_minted = fractional_quantity.to_u256_18_decimals()?;

                Ok(MintWorkflowOutcome::TokensReceived {
                    accepted_event,
                    tx_hash,
                    shares_minted,
                })
            }
            status @ (TokenizationRequestStatus::Rejected | TokenizationRequestStatus::Pending) => {
                Ok(MintWorkflowOutcome::AcceptanceFailed {
                    accepted_event,
                    last_status: status,
                })
            }
        }
    }

    fn outcome_to_events(
        outcome: MintWorkflowOutcome,
        requested: TokenizedEquityMintEvent,
        symbol: Symbol,
        quantity: Decimal,
        fractional_quantity: FractionalShares,
    ) -> Vec<TokenizedEquityMintEvent> {
        match outcome {
            MintWorkflowOutcome::Rejected { status_code } => {
                vec![
                    requested,
                    TokenizedEquityMintEvent::MintRejected {
                        symbol,
                        quantity,
                        status_code,
                        rejected_at: Utc::now(),
                    },
                ]
            }
            MintWorkflowOutcome::AcceptanceFailed {
                accepted_event,
                last_status,
            } => {
                vec![
                    requested,
                    accepted_event,
                    TokenizedEquityMintEvent::MintAcceptanceFailed {
                        symbol,
                        quantity,
                        last_status,
                        failed_at: Utc::now(),
                    },
                ]
            }
            MintWorkflowOutcome::TokensReceived {
                accepted_event,
                tx_hash,
                shares_minted,
            } => {
                vec![
                    requested,
                    accepted_event,
                    TokenizedEquityMintEvent::TokensReceived {
                        symbol,
                        quantity: fractional_quantity.inner(),
                        tx_hash,
                        receipt_id: ReceiptId(U256::ZERO),
                        shares_minted,
                        received_at: Utc::now(),
                    },
                ]
            }
        }
    }

    /// Handles the Deposit command: deposits received tokens into Raindex vault.
    ///
    /// Flow: DepositedIntoRaindex -> Completed (success)
    ///    or RaindexDepositFailed (failure, terminal)
    async fn handle_deposit(
        &self,
        services: &MintServices,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        let (symbol, quantity, shares_minted) = match self.live() {
            Ok(TokenizedEquityMint::TokensReceived {
                symbol,
                quantity,
                shares_minted,
                ..
            }) => (symbol.clone(), *quantity, *shares_minted),
            Err(LifecycleError::Uninitialized)
            | Ok(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. },
            ) => return Err(TokenizedEquityMintError::TokensNotReceived),
            Ok(
                TokenizedEquityMint::DepositedIntoRaindex { .. }
                | TokenizedEquityMint::Completed { .. },
            ) => return Err(TokenizedEquityMintError::AlreadyCompleted),
            Ok(TokenizedEquityMint::Failed { .. }) => {
                return Err(TokenizedEquityMintError::AlreadyFailed);
            }
            Err(e) => return Err(e.into()),
        };

        let (token, vault_id) = services.raindex.lookup_vault_info(&symbol).await?;

        match services
            .raindex
            .deposit(token, vault_id, shares_minted, TOKENIZED_EQUITY_DECIMALS)
            .await
        {
            Ok(vault_deposit_tx_hash) => Ok(vec![
                TokenizedEquityMintEvent::DepositedIntoRaindex {
                    symbol: symbol.clone(),
                    quantity,
                    vault_deposit_tx_hash,
                    deposited_at: Utc::now(),
                },
                TokenizedEquityMintEvent::Completed {
                    symbol,
                    quantity,
                    completed_at: Utc::now(),
                },
            ]),
            Err(_) => Ok(vec![TokenizedEquityMintEvent::RaindexDepositFailed {
                symbol,
                quantity,
                failed_tx_hash: None,
                failed_at: Utc::now(),
            }]),
        }
    }
}

impl TokenizedEquityMint {
    /// Apply a transition event to an existing mint state.
    pub(crate) fn apply_transition(
        event: &TokenizedEquityMintEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            TokenizedEquityMintEvent::MintRequested { .. } => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
            }),
            TokenizedEquityMintEvent::MintRejected { rejected_at, .. } => {
                current.apply_rejected(*rejected_at, event)
            }

            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
                ..
            } => current.apply_accepted(
                issuer_request_id,
                tokenization_request_id,
                *accepted_at,
                event,
            ),
            TokenizedEquityMintEvent::MintAcceptanceFailed { failed_at, .. } => {
                current.apply_acceptance_failed(*failed_at, event)
            }

            TokenizedEquityMintEvent::TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
                ..
            } => current.apply_tokens_received(
                *tx_hash,
                receipt_id,
                *shares_minted,
                *received_at,
                event,
            ),
            TokenizedEquityMintEvent::DepositedIntoRaindex {
                vault_deposit_tx_hash,
                deposited_at,
                ..
            } => current.apply_vault_deposited(*vault_deposit_tx_hash, *deposited_at, event),
            TokenizedEquityMintEvent::RaindexDepositFailed { failed_at, .. } => {
                current.apply_vault_deposit_failed(*failed_at, event)
            }
            TokenizedEquityMintEvent::Completed { completed_at, .. } => {
                current.apply_completed(*completed_at, event)
            }
        }
    }

    /// Create initial state from an initialization event.
    pub(crate) fn from_event(
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            TokenizedEquityMintEvent::MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => Ok(Self::MintRequested {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                requested_at: *requested_at,
            }),

            _ => Err(LifecycleError::Mismatch {
                state: "Uninitialized".into(),
                event: format!("{event:?}"),
            }),
        }
    }

    fn apply_accepted(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
        accepted_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::MintRequested {
            symbol,
            quantity,
            wallet,
            requested_at,
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::MintAccepted {
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            requested_at: *requested_at,
            accepted_at,
        })
    }

    fn apply_tokens_received(
        &self,
        tx_hash: TxHash,
        receipt_id: &ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
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
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::TokensReceived {
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

    fn apply_vault_deposited(
        &self,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensReceived {
            symbol,
            quantity,
            issuer_request_id,
            tokenization_request_id,
            tx_hash,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::DepositedIntoRaindex {
            symbol: symbol.clone(),
            quantity: *quantity,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            token_tx_hash: *tx_hash,
            vault_deposit_tx_hash,
            deposited_at,
        })
    }

    fn apply_vault_deposit_failed(
        &self,
        failed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::TokensReceived {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            requested_at: *requested_at,
            failed_at,
        })
    }

    fn apply_completed(
        &self,
        completed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::DepositedIntoRaindex {
            symbol,
            quantity,
            issuer_request_id,
            tokenization_request_id,
            token_tx_hash,
            vault_deposit_tx_hash,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Completed {
            symbol: symbol.clone(),
            quantity: *quantity,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            token_tx_hash: *token_tx_hash,
            vault_deposit_tx_hash: *vault_deposit_tx_hash,
            completed_at,
        })
    }

    fn apply_rejected(
        &self,
        rejected_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::MintRequested {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            requested_at: *requested_at,
            failed_at: rejected_at,
        })
    }

    fn apply_acceptance_failed(
        &self,
        failed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let Self::MintAccepted {
            symbol,
            quantity,
            requested_at,
            ..
        } = self
        else {
            return Err(LifecycleError::Mismatch {
                state: format!("{self:?}"),
                event: event.event_type(),
            });
        };

        Ok(Self::Failed {
            symbol: symbol.clone(),
            quantity: *quantity,
            requested_at: *requested_at,
            failed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::mock::MockTokenizer;

    fn mock_services() -> MintServices {
        MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            raindex: Arc::new(MockRaindex::new()),
        }
    }

    #[test]
    fn event_application_rejects_mint_accepted_on_completed_state() {
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
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS999".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK999".to_string()),
            accepted_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &completed).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("Completed"));
        assert_eq!(evt, "TokenizedEquityMintEvent::MintAccepted");
    }

    #[test]
    fn event_application_rejects_tokens_received_on_requested_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::TokensReceived {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &requested).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintRequested"));
        assert_eq!(evt, "TokenizedEquityMintEvent::TokensReceived");
    }

    #[test]
    fn event_application_rejects_completed_on_accepted_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            completed_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::Completed");
    }

    #[test]
    fn event_application_rejects_rejected_on_accepted_state() {
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
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            status_code: None,
            rejected_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::MintRejected");
    }

    #[test]
    fn event_application_rejects_acceptance_failed_on_requested_state() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            last_status: TokenizationRequestStatus::Pending,
            failed_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &requested).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintRequested"));
        assert_eq!(evt, "TokenizedEquityMintEvent::MintAcceptanceFailed");
    }

    #[test]
    fn event_application_rejects_deposited_into_raindex_on_accepted_state() {
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
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::DepositedIntoRaindex");
    }

    #[test]
    fn event_application_rejects_raindex_deposit_failed_on_accepted_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::RaindexDepositFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            failed_tx_hash: None,
            failed_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::RaindexDepositFailed");
    }

    #[tokio::test]
    async fn mint_command_rejects_when_already_in_progress() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: IssuerRequestId::new("ISS123"),
                    symbol,
                    quantity: dec!(50.0),
                    wallet,
                },
                &mock_services(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyInProgress)
        ));
    }

    #[tokio::test]
    async fn mint_command_rejects_when_already_completed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(1)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::DepositedIntoRaindex {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::Completed {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            completed_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: IssuerRequestId::new("ISS456"),
                    symbol,
                    quantity: dec!(50.0),
                    wallet,
                },
                &mock_services(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn mint_command_rejects_when_already_failed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::MintRejected {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            status_code: None,
            rejected_at: Utc::now(),
        });

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: IssuerRequestId::new("ISS456"),
                    symbol,
                    quantity: dec!(50.0),
                    wallet,
                },
                &mock_services(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyFailed)
        ));
    }

    #[tokio::test]
    async fn deposit_command_rejects_before_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::MintAccepted {
            symbol,
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        });

        let result = aggregate
            .handle(TokenizedEquityMintCommand::Deposit, &mock_services())
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::TokensNotReceived)
        ));
    }

    #[tokio::test]
    async fn deposit_command_rejects_when_already_completed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(1)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::DepositedIntoRaindex {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            vault_deposit_tx_hash: TxHash::random(),
            deposited_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::Completed {
            symbol,
            quantity: dec!(100.5),
            completed_at: Utc::now(),
        });

        let result = aggregate
            .handle(TokenizedEquityMintCommand::Deposit, &mock_services())
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn deposit_command_rejects_when_already_failed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        aggregate.apply(TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        });
        aggregate.apply(TokenizedEquityMintEvent::MintRejected {
            symbol,
            quantity: dec!(100.5),
            status_code: None,
            rejected_at: Utc::now(),
        });

        let result = aggregate
            .handle(TokenizedEquityMintCommand::Deposit, &mock_services())
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyFailed)
        ));
    }
}
