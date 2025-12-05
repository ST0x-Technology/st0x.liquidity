//! Tokenized Equity Mint aggregate for converting offchain Alpaca shares to onchain tokens.
//!
//! This module implements the CQRS-ES aggregate pattern for managing the asynchronous workflow
//! of minting tokenized equity shares. It tracks the complete lifecycle from requesting a mint
//! through Alpaca's tokenization API to receiving the onchain tokens.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//! (start) --RequestMint--> MintRequested --AcknowledgeAcceptance--> MintAccepted
//!                                |                                       |
//!                                |                                       |
//!                                v                                       v
//!                             Failed <-------Fail------- TokensReceived
//!                                                              |
//!                                                              | Finalize
//!                                                              v
//!                                                          Completed
//! ```
//!
//! - `MintRequested`, `MintAccepted`, and `TokensReceived` can transition to `Failed`
//! - `Completed` and `Failed` are terminal states
//!
//! # Alpaca API Integration
//!
//! The mint process integrates with Alpaca's tokenization API:
//!
//! 1. **Request**: System initiates mint request with symbol, quantity, and destination wallet
//! 2. **Acceptance**: Alpaca responds with `issuer_request_id` and `tokenization_request_id`
//! 3. **Transfer**: Alpaca executes onchain transfer, system detects transaction
//! 4. **Completion**: System verifies receipt and finalizes mint
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
use cqrs_es::{Aggregate, DomainEvent, EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

use crate::lifecycle::{Lifecycle, LifecycleError, Never};

/// Alpaca issuer request identifier returned when a tokenization request is accepted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssuerRequestId(pub(crate) String);

/// Alpaca tokenization request identifier used to track the mint operation through their API.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

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
    /// Attempted to finalize before tokens were received
    #[error("Cannot finalize: tokens not received")]
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
}

#[derive(Debug, Clone)]
pub(crate) enum TokenizedEquityMintCommand {
    RequestMint {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    },
    AcknowledgeAcceptance {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
    },
    ReceiveTokens {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
    },
    Finalize,
    Fail {
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedEquityMintEvent {
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    MintAccepted {
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    },
    TokensReceived {
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    },
    MintCompleted {
        completed_at: DateTime<Utc>,
    },
    MintFailed {
        reason: String,
        failed_at: DateTime<Utc>,
    },
}

impl DomainEvent for TokenizedEquityMintEvent {
    fn event_type(&self) -> String {
        match self {
            Self::MintRequested { .. } => "TokenizedEquityMintEvent::MintRequested".to_string(),
            Self::MintAccepted { .. } => "TokenizedEquityMintEvent::MintAccepted".to_string(),
            Self::TokensReceived { .. } => "TokenizedEquityMintEvent::TokensReceived".to_string(),
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
            Self::MintFailed { .. } => "TokenizedEquityMintEvent::MintFailed".to_string(),
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

    /// Mint operation successfully completed (terminal state)
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        requested_at: DateTime<Utc>,
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

impl TokenizedEquityMint {
    /// Apply a transition event to an existing mint state.
    pub(crate) fn apply_transition(
        event: &TokenizedEquityMintEvent,
        current: &Self,
    ) -> Result<Self, LifecycleError<Never>> {
        match event {
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => current.apply_accepted(
                issuer_request_id,
                tokenization_request_id,
                *accepted_at,
                event,
            ),

            TokenizedEquityMintEvent::TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => current.apply_tokens_received(
                *tx_hash,
                receipt_id,
                *shares_minted,
                *received_at,
                event,
            ),

            TokenizedEquityMintEvent::MintCompleted { completed_at } => {
                current.apply_completed(*completed_at, event)
            }

            TokenizedEquityMintEvent::MintFailed { reason, failed_at } => {
                current.apply_failed(reason, *failed_at, event)
            }

            TokenizedEquityMintEvent::MintRequested { .. } => Err(LifecycleError::Mismatch {
                state: format!("{current:?}"),
                event: event.event_type(),
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

    fn apply_completed(
        &self,
        completed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
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
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            tx_hash: *tx_hash,
            receipt_id: receipt_id.clone(),
            shares_minted: *shares_minted,
            requested_at: *requested_at,
            completed_at,
        })
    }

    fn apply_failed(
        &self,
        reason: &str,
        failed_at: DateTime<Utc>,
        event: &TokenizedEquityMintEvent,
    ) -> Result<Self, LifecycleError<Never>> {
        let (Self::MintRequested {
            symbol,
            quantity,
            requested_at,
            ..
        }
        | Self::MintAccepted {
            symbol,
            quantity,
            requested_at,
            ..
        }
        | Self::TokensReceived {
            symbol,
            quantity,
            requested_at,
            ..
        }) = self
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
            TokenizedEquityMintCommand::AcknowledgeAcceptance {
                issuer_request_id,
                tokenization_request_id,
            } => self.handle_acknowledge_acceptance(issuer_request_id, tokenization_request_id),
            TokenizedEquityMintCommand::ReceiveTokens {
                tx_hash,
                receipt_id,
                shares_minted,
            } => self.handle_receive_tokens(*tx_hash, receipt_id, *shares_minted),
            TokenizedEquityMintCommand::Finalize => self.handle_finalize(),
            TokenizedEquityMintCommand::Fail { reason } => self.handle_fail(reason),
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

    fn handle_acknowledge_acceptance(
        &self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Err(TokenizedEquityMintError::NotRequested),
            Ok(TokenizedEquityMint::MintRequested { .. }) => {
                Ok(vec![TokenizedEquityMintEvent::MintAccepted {
                    issuer_request_id: issuer_request_id.clone(),
                    tokenization_request_id: tokenization_request_id.clone(),
                    accepted_at: Utc::now(),
                }])
            }
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
            Ok(TokenizedEquityMint::MintAccepted { .. }) => {
                Ok(vec![TokenizedEquityMintEvent::TokensReceived {
                    tx_hash,
                    receipt_id: receipt_id.clone(),
                    shares_minted,
                    received_at: Utc::now(),
                }])
            }
            Ok(
                TokenizedEquityMint::Completed { .. } | TokenizedEquityMint::TokensReceived { .. },
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
                | TokenizedEquityMint::MintAccepted { .. },
            ) => Err(TokenizedEquityMintError::TokensNotReceived),
            Ok(TokenizedEquityMint::TokensReceived { .. }) => {
                Ok(vec![TokenizedEquityMintEvent::MintCompleted {
                    completed_at: Utc::now(),
                }])
            }
            Ok(TokenizedEquityMint::Completed { .. }) => {
                Err(TokenizedEquityMintError::AlreadyCompleted)
            }
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }

    fn handle_fail(
        &self,
        reason: &str,
    ) -> Result<Vec<TokenizedEquityMintEvent>, TokenizedEquityMintError> {
        match self.live() {
            Err(LifecycleError::Uninitialized) => Err(TokenizedEquityMintError::NotRequested),
            Ok(
                TokenizedEquityMint::MintRequested { .. }
                | TokenizedEquityMint::MintAccepted { .. }
                | TokenizedEquityMint::TokensReceived { .. },
            ) => Ok(vec![TokenizedEquityMintEvent::MintFailed {
                reason: reason.to_string(),
                failed_at: Utc::now(),
            }]),
            Ok(TokenizedEquityMint::Completed { .. }) => {
                Err(TokenizedEquityMintError::AlreadyCompleted)
            }
            Ok(TokenizedEquityMint::Failed { .. }) => Err(TokenizedEquityMintError::AlreadyFailed),
            Err(e) => Err(e.into()),
        }
    }
}

impl View<Self> for Lifecycle<TokenizedEquityMint, Never> {
    fn update(&mut self, event: &EventEnvelope<Self>) {
        *self = self
            .clone()
            .transition(&event.payload, TokenizedEquityMint::apply_transition)
            .or_initialize(&event.payload, TokenizedEquityMint::from_event);
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
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
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
    async fn test_finalize_after_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

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
    async fn test_cannot_finalize_before_tokens_received() {
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

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let result = aggregate
            .handle(TokenizedEquityMintCommand::Finalize, &())
            .await;

        assert!(matches!(
            result,
            Err(TokenizedEquityMintError::TokensNotReceived)
        ));
    }

    #[tokio::test]
    async fn test_fail_from_requested_state() {
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
                TokenizedEquityMintCommand::Fail {
                    reason: "Alpaca API timeout".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_fail_from_accepted_state() {
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

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::Fail {
                    reason: "Transaction reverted".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            TokenizedEquityMintEvent::MintFailed { .. }
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_when_completed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint, Never>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();
        let tx_hash = TxHash::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let accepted_event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            accepted_at: Utc::now(),
        };
        aggregate.apply(accepted_event);

        let received_event = TokenizedEquityMintEvent::TokensReceived {
            tx_hash,
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };
        aggregate.apply(received_event);

        let completed_event = TokenizedEquityMintEvent::MintCompleted {
            completed_at: Utc::now(),
        };
        aggregate.apply(completed_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::Fail {
                    reason: "Cannot fail completed mint".to_string(),
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
    async fn test_cannot_fail_when_already_failed() {
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

        let failed_event = TokenizedEquityMintEvent::MintFailed {
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        };
        aggregate.apply(failed_event);

        let result = aggregate
            .handle(
                TokenizedEquityMintCommand::Fail {
                    reason: "Cannot fail again".to_string(),
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
    fn test_apply_accepted_rejects_wrong_state() {
        let completed = TokenizedEquityMint::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            requested_at: Utc::now(),
            completed_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintAccepted {
            issuer_request_id: IssuerRequestId("ISS999".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK999".to_string()),
            accepted_at: Utc::now(),
        };

        let result = TokenizedEquityMint::apply_transition(&event, &completed);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
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
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            received_at: Utc::now(),
        };

        let result = TokenizedEquityMint::apply_transition(&event, &requested);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
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
            completed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::apply_transition(&event, &accepted);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }

    #[test]
    fn test_apply_failed_rejects_terminal_states() {
        let completed = TokenizedEquityMint::Completed {
            symbol: Symbol::new("AAPL").unwrap(),
            quantity: dec!(100.5),
            wallet: Address::random(),
            issuer_request_id: IssuerRequestId("ISS123".to_string()),
            tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
            tx_hash: TxHash::random(),
            receipt_id: ReceiptId(U256::from(789)),
            shares_minted: U256::from(100_500_000_000_000_000_000_u128),
            requested_at: Utc::now(),
            completed_at: Utc::now(),
        };

        let event = TokenizedEquityMintEvent::MintFailed {
            reason: "Should not apply".to_string(),
            failed_at: Utc::now(),
        };

        let result = TokenizedEquityMint::apply_transition(&event, &completed);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
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

        let result = TokenizedEquityMint::apply_transition(&event, &requested);

        assert!(matches!(result, Err(LifecycleError::Mismatch { .. })));
    }
}
