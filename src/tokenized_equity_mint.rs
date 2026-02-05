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
//! MintRequested -> MintAccepted -> TokensReceived -> VaultDeposited -> Completed
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
use st0x_execution::Symbol;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

/// SQLite-backed event store for TokenizedEquityMint aggregates.
pub(crate) type MintEventStore =
    PersistedEventStore<SqliteEventRepository, Lifecycle<TokenizedEquityMint, Never>>;

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

/// Errors that can occur during tokenized equity mint operations.
///
/// These errors enforce state machine constraints and prevent invalid transitions.
#[derive(Debug, thiserror::Error)]
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
    /// Attempted to deposit to vault before tokens were received
    #[error("Cannot deposit to vault: tokens not received")]
    TokensNotReceived,
    /// Attempted to finalize before vault deposit
    #[error("Cannot finalize: vault deposit not complete")]
    VaultDepositNotComplete,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
    /// Lifecycle state error
    #[error(transparent)]
    State(#[from] LifecycleError<Never>),
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

    /// Deposit tokens from wallet to Raindex vault.
    DepositToVault {
        vault_deposit_tx_hash: TxHash,
    },

    Finalize,
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
        reason: String,
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
        reason: String,
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
    VaultDeposited {
        symbol: Symbol,
        quantity: Decimal,
        vault_deposit_tx_hash: TxHash,
        deposited_at: DateTime<Utc>,
    },
    /// Vault deposit failed after tokens were received.
    /// Tokens remain in wallet, can be retried or manually recovered.
    VaultDepositFailed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        failed_at: DateTime<Utc>,
    },

    MintCompleted {
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
            Self::VaultDeposited { .. } => "TokenizedEquityMintEvent::VaultDeposited".to_string(),
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
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
    VaultDeposited {
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

    /// Mint operation failed with error reason (terminal state)
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

#[async_trait]
impl Aggregate for Lifecycle<TokenizedEquityMint, Never> {
    type Command = TokenizedEquityMintCommand;
    type Event = TokenizedEquityMintEvent;
    type Error = TokenizedEquityMintError;
    type Services = ();

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
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match &command {
            TokenizedEquityMintCommand::RequestMint {
                symbol,
                quantity,
                wallet,
            } => self.handle_request_mint(symbol, *quantity, *wallet),
            TokenizedEquityMintCommand::RejectMint { reason } => self.handle_reject_mint(reason),

            TokenizedEquityMintCommand::AcknowledgeAcceptance {
                issuer_request_id,
                tokenization_request_id,
            } => self.handle_acknowledge_acceptance(issuer_request_id, tokenization_request_id),
            TokenizedEquityMintCommand::FailAcceptance { reason } => {
                self.handle_fail_acceptance(reason)
            }

            TokenizedEquityMintCommand::ReceiveTokens {
                tx_hash,
                receipt_id,
                shares_minted,
            } => self.handle_receive_tokens(*tx_hash, receipt_id, *shares_minted),

            TokenizedEquityMintCommand::DepositToVault {
                vault_deposit_tx_hash,
            } => self.handle_deposit_to_vault(*vault_deposit_tx_hash),
            TokenizedEquityMintCommand::FailVaultDeposit { reason } => {
                self.handle_fail_vault_deposit(reason)
            }

            TokenizedEquityMintCommand::Finalize => self.handle_finalize(),
        }
    }
}

impl Lifecycle<TokenizedEquityMint, Never> {
    fn handle_request_mint(
        &self,
        symbol: &Symbol,
        quantity: Decimal,
        wallet: Address,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => {
                Ok(vec![TokenizedEquityMintEvent::MintRequested {
                    symbol: symbol.clone(),
                    quantity,
                    wallet,
                    requested_at: Utc::now(),
                }])
            }
            Ok(_) => Err(TokenizedEquityMintError::AlreadyInProgress),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_reject_mint(
        &self,
        reason: &str,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Err(TokenizedEquityMintError::NotRequested),
            Ok(TokenizedEquityMint::MintRequested {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::MintRejected {
                symbol: symbol.clone(),
                quantity: *quantity,
                reason: reason.to_string(),
                rejected_at: Utc::now(),
            }]),
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Ok(_) => Err(TokenizedEquityMintError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_acknowledge_acceptance(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Ok(TokenizedEquityMint::MintRequested {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::MintAccepted {
                symbol: symbol.clone(),
                quantity: *quantity,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                accepted_at: Utc::now(),
            }]),

            Err(LifecycleError::Uninitialized) => Err(TokenizedEquityMintError::NotRequested),
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Ok(_) => Err(TokenizedEquityMintError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail_acceptance(
        &self,
        reason: &str,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) | Ok(TokenizedEquityMint::MintRequested { .. }) => {
                Err(TokenizedEquityMintError::NotAccepted)
            }

            Ok(TokenizedEquityMint::MintAccepted {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::MintAcceptanceFailed {
                symbol: symbol.clone(),
                quantity: *quantity,
                reason: reason.to_string(),
                failed_at: Utc::now(),
            }]),

            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Ok(_) => Err(TokenizedEquityMintError::AlreadyCompleted),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_receive_tokens(
        &self,
        tx_hash: TxHash,
        receipt_id: &ReceiptId,
        shares_minted: U256,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) | Ok(TokenizedEquityMint::MintRequested { .. }) => {
                Err(TokenizedEquityMintError::NotAccepted)
            }

            Ok(TokenizedEquityMint::MintAccepted {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::TokensReceived {
                symbol: symbol.clone(),
                quantity: *quantity,
                tx_hash,
                receipt_id: receipt_id.clone(),
                shares_minted,
                received_at: Utc::now(),
            }]),

            Ok(
                TokenizedEquityMint::Completed { .. }
                | TokenizedEquityMint::TokensReceived { .. }
                | TokenizedEquityMint::VaultDeposited { .. },
            ) => Err(TokenizedEquityMintError::AlreadyCompleted),

            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_deposit_to_vault(
        &self,
        vault_deposit_tx_hash: TxHash,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. },
            ) => Err(TokenizedEquityMintError::TokensNotReceived),

            Ok(TokenizedEquityMint::TokensReceived {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::VaultDeposited {
                symbol: symbol.clone(),
                quantity: *quantity,
                vault_deposit_tx_hash,
                deposited_at: Utc::now(),
            }]),
            Ok(
                TokenizedEquityMint::VaultDeposited { .. } | TokenizedEquityMint::Completed { .. },
            ) => Err(TokenizedEquityMintError::AlreadyCompleted),
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_finalize(&self) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized)
            | Ok(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. }
                | TokenizedEquityMint::TokensReceived { .. },
            ) => Err(TokenizedEquityMintError::VaultDepositNotComplete),

            Ok(TokenizedEquityMint::VaultDeposited {
                symbol, quantity, ..
            }) => Ok(vec![TokenizedEquityMintEvent::MintCompleted {
                symbol: symbol.clone(),
                quantity: *quantity,
                completed_at: Utc::now(),
            }]),

            Ok(TokenizedEquityMint::Completed { .. }) => {
                Err(TokenizedEquityMintError::AlreadyCompleted)
            }

            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Err(e) => Err(e.into()),
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
            TokenizedEquityMintEvent::MintRejected {
                reason,
                rejected_at,
                ..
            } => current.apply_rejected(reason, *rejected_at, event),

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
            TokenizedEquityMintEvent::MintAcceptanceFailed {
                reason, failed_at, ..
            } => current.apply_acceptance_failed(reason, *failed_at, event),

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
            TokenizedEquityMintEvent::VaultDeposited {
                vault_deposit_tx_hash,
                deposited_at,
                ..
            } => current.apply_vault_deposited(*vault_deposit_tx_hash, *deposited_at, event),
            TokenizedEquityMintEvent::VaultDepositFailed {
                reason, failed_at, ..
            } => current.apply_vault_deposit_failed(reason, *failed_at, event),
            TokenizedEquityMintEvent::MintCompleted { completed_at, .. } => {
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

        Ok(Self::VaultDeposited {
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
        reason: &str,
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
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }

    fn apply_completed(
        &self,
        completed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
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
        reason: &str,
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
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at: rejected_at,
        })
    }

    fn apply_acceptance_failed(
        &self,
        reason: &str,
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
            reason: reason.to_string(),
            requested_at: *requested_at,
            failed_at,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_request_mint_from_uninitialized() {
        let aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::RequestMint {
                    symbol: symbol.clone(),
                    quantity: dec!(100.5),
                    wallet,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintRequested { .. }
        ));
    }

    #[tokio::test]
    async fn test_acknowledge_acceptance_after_request() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id: IssuerRequestId("ISS123".to_string()),
                    tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintAccepted { .. }
        ));
    }

    #[tokio::test]
    async fn test_receive_tokens_after_acceptance() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol,
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::ReceiveTokens {
                    tx_hash,
                    receipt_id: ReceiptId(U256::from(789)),
                    shares_minted: U256::from(100_500_000_000_000_000_000_u128),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::TokensReceived { .. }
        ));
    }

    #[tokio::test]
    async fn test_finalize_after_vault_deposit() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();
        let vault_deposit_tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let deposited_event = TokenizedEquityMintEvent::VaultDeposited {
            symbol,
            quantity: dec!(100.5),
            vault_deposit_tx_hash,
            deposited_at: Utc::now(),
        };
        aggregate.apply(deposited_event);

        let events = aggregate
            .handle(TokenizedEquityMintCommand::Finalize, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintCompleted { .. }
        ));
    }

    #[tokio::test]
    async fn test_complete_mint_flow_end_to_end() {
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();
        let vault_deposit_tx_hash = TxHash::random();

        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::RequestMint {
                    symbol: symbol.clone(),
                    quantity: dec!(100.5),
                    wallet,
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id: IssuerRequestId("ISS123".to_string()),
                    tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::ReceiveTokens {
                    tx_hash,
                    receipt_id: ReceiptId(U256::from(789)),
                    shares_minted: U256::from(100_500_000_000_000_000_000_u128),
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash,
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        let events = aggregate
            .handle(TokenizedEquityMintCommand::Finalize, &())
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        for event in events {
            aggregate.apply(event);
        }

        assert!(matches!(
            aggregate,
            Lifecycle::Live(TokenizedEquityMint::Completed { .. })
        ));
    }

    #[tokio::test]
    async fn test_cannot_acknowledge_before_request() {
        let aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id: IssuerRequestId("ISS123".to_string()),
                    tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::NotRequested)
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_tokens_before_acceptance() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::ReceiveTokens {
                    tx_hash: TxHash::random(),
                    receipt_id: ReceiptId(U256::from(789)),
                    shares_minted: U256::from(100_500_000_000_000_000_000_u128),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(TokenizedEquityMintError::NotAccepted)));
    }

    #[tokio::test]
    async fn test_cannot_finalize_before_vault_deposit() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            symbol,
            quantity: dec!(100.5),
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let result = aggregate
            .handle(TokenizedEquityMintCommand::Finalize, &())
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::VaultDepositNotComplete)
        ));
    }

    #[tokio::test]
    async fn test_reject_mint_from_requested_state() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::RejectMint {
                    reason: "Alpaca API timeout".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintRejected { .. }
        ));
    }

    #[tokio::test]
    async fn test_fail_acceptance_from_accepted_state() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol,
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "Transaction reverted".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintAcceptanceFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_mint_when_completed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();
        let vault_deposit_tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let deposited_event = TokenizedEquityMintEvent::VaultDeposited {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            vault_deposit_tx_hash,
            deposited_at: Utc::now(),
        };
        aggregate.apply(deposited_event);

        let completed_event = TokenizedEquityMintEvent::MintCompleted {
            symbol,
            quantity: dec!(100.5),
            completed_at: Utc::now(),
        };
        aggregate.apply(completed_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::RejectMint {
                    reason: "Cannot reject completed mint".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_mint_when_already_failed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let rejected_event = TokenizedEquityMintEvent::MintRejected {
            symbol,
            quantity: dec!(100.5),
            reason: "First rejection".to_string(),
            rejected_at: Utc::now(),
        };
        aggregate.apply(rejected_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::RejectMint {
                    reason: "Cannot reject again".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyFailed)
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_mint_before_request() {
        let aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::RejectMint {
                    reason: "Cannot reject uninitialized".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::NotRequested)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_acceptance_before_acceptance() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::FailAcceptance {
                    reason: "Cannot fail before acceptance".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(TokenizedEquityMintError::NotAccepted)));
    }

    #[test]
    fn test_apply_accepted_rejects_wrong_state() {
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
    fn test_apply_tokens_received_rejects_wrong_state() {
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
    fn test_apply_completed_rejects_wrong_state() {
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
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            completed_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::MintCompleted");
    }

    #[test]
    fn test_apply_rejected_rejects_non_requested_states() {
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
            reason: "Should not apply".to_string(),
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
    fn test_apply_acceptance_failed_rejects_non_accepted_states() {
        let requested = TokenizedEquityMint::MintRequested {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            requested_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAcceptanceFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            reason: "Should not apply".to_string(),
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
    fn test_apply_transition_rejects_mint_requested_event() {
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

        let err = TokenizedEquityMint::apply_transition(&event, &requested).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintRequested"));
        assert_eq!(evt, "TokenizedEquityMintEvent::MintRequested");
    }

    #[tokio::test]
    async fn test_cannot_deposit_to_vault_before_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol,
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::random(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::TokensNotReceived)
        ));
    }

    #[tokio::test]
    async fn test_cannot_deposit_to_vault_when_already_deposited() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();
        let vault_deposit_tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let deposited_event = TokenizedEquityMintEvent::VaultDeposited {
            symbol,
            quantity: dec!(100.5),
            vault_deposit_tx_hash,
            deposited_at: Utc::now(),
        };
        aggregate.apply(deposited_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::random(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_deposit_to_vault_when_failed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let rejected_event = TokenizedEquityMintEvent::MintRejected {
            symbol,
            quantity: dec!(100.5),
            reason: "Rejected by Alpaca".to_string(),
            rejected_at: Utc::now(),
        };
        aggregate.apply(rejected_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash: TxHash::random(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::AlreadyFailed)
        ));
    }

    #[test]
    fn test_apply_vault_deposited_rejects_wrong_state() {
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
        assert_eq!(evt, "TokenizedEquityMintEvent::VaultDeposited");
    }

    #[tokio::test]
    async fn test_fail_vault_deposit_from_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            symbol,
            quantity: dec!(100.5),
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::FailVaultDeposit {
                    reason: "Vault not found".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::VaultDepositFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_vault_deposit_before_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol: symbol.clone(),
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            symbol,
            quantity: dec!(100.5),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::FailVaultDeposit {
                    reason: "Should not work".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::TokensNotReceived)
        ));
    }

    #[test]
    fn test_apply_vault_deposit_failed_rejects_wrong_state() {
        let accepted = TokenizedEquityMint::MintAccepted {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            requested_at: Utc::now(),
            accepted_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::VaultDepositFailed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        let err = TokenizedEquityMint::apply_transition(&event, &accepted).unwrap_err();

        let LifecycleError::Mismatch { state, event: evt } = err else {
            panic!("Expected Mismatch error, got {err:?}");
        };
        assert!(state.contains("MintAccepted"));
        assert_eq!(evt, "TokenizedEquityMintEvent::VaultDepositFailed");
    }
}
