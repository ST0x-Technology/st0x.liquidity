//! Aggregate modeling the lifecycle of minting tokenized
//! equities from underlying Alpaca shares.
//!
//! Tracks the workflow from requesting a mint through
//! Alpaca's tokenization API to receiving onchain tokens.
//!
//! # State Flow
//!
//! The aggregate progresses through the following states:
//!
//! ```text
//! (start) --RequestMint--> MintRequested --AcknowledgeAcceptance--> MintAccepted
//!                                |                                       |
//!                                | RejectMint                            | FailAcceptance
//!                                v                                       v
//!                             Failed                             TokensReceived
//!                                                                       |
//!                                                                       | Finalize
//!                                                                       v
//!                                                                   Completed
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
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_execution::Symbol;
use std::str::FromStr;

use st0x_event_sorcery::{DomainEvent, EventSourced};

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

#[async_trait]
impl EventSourced for TokenizedEquityMint {
    type Id = IssuerRequestId;
    type Event = TokenizedEquityMintEvent;
    type Command = TokenizedEquityMintCommand;
    type Error = TokenizedEquityMintError;
    type Services = ();

    const AGGREGATE_TYPE: &'static str = "TokenizedEquityMint";
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

    fn evolve(event: &Self::Event, state: &Self) -> Result<Option<Self>, Self::Error> {
        use TokenizedEquityMintEvent::*;
        Ok(match event {
            MintRequested { .. } => None,
            MintRejected {
                reason,
                rejected_at,
            } => state.try_apply_rejected(reason, *rejected_at),
            MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => state.try_apply_accepted(issuer_request_id, tokenization_request_id, *accepted_at),
            MintAcceptanceFailed { reason, failed_at } => {
                state.try_apply_acceptance_failed(reason, *failed_at)
            }
            TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => {
                state.try_apply_tokens_received(*tx_hash, receipt_id, *shares_minted, *received_at)
            }
            MintCompleted { completed_at } => state.try_apply_completed(*completed_at),
        })
    }

    async fn initialize(
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintCommand::*;
        use TokenizedEquityMintEvent::*;
        match command {
            RequestMint {
                symbol,
                quantity,
                wallet,
            } => Ok(vec![MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at: Utc::now(),
            }]),
            RejectMint { .. } | AcknowledgeAcceptance { .. } => {
                Err(TokenizedEquityMintError::NotRequested)
            }
            FailAcceptance { .. } | ReceiveTokens { .. } => {
                Err(TokenizedEquityMintError::NotAccepted)
            }
            Finalize => Err(TokenizedEquityMintError::TokensNotReceived),
        }
    }

    async fn transition(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        use TokenizedEquityMintCommand::*;
        use TokenizedEquityMintEvent::*;
        match command {
            RequestMint { .. } => Err(TokenizedEquityMintError::AlreadyInProgress),

            RejectMint { reason } => match self {
                Self::MintRequested { .. } => Ok(vec![MintRejected {
                    reason,
                    rejected_at: Utc::now(),
                }]),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyCompleted),
            },

            AcknowledgeAcceptance {
                issuer_request_id,
                tokenization_request_id,
            } => match self {
                Self::MintRequested { .. } => Ok(vec![MintAccepted {
                    issuer_request_id,
                    tokenization_request_id,
                    accepted_at: Utc::now(),
                }]),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyCompleted),
            },

            FailAcceptance { reason } => match self {
                Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
                Self::MintAccepted { .. } => Ok(vec![MintAcceptanceFailed {
                    reason,
                    failed_at: Utc::now(),
                }]),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
                _ => Err(TokenizedEquityMintError::AlreadyCompleted),
            },

            ReceiveTokens {
                tx_hash,
                receipt_id,
                shares_minted,
            } => match self {
                Self::MintRequested { .. } => Err(TokenizedEquityMintError::NotAccepted),
                Self::MintAccepted { .. } => Ok(vec![TokensReceived {
                    tx_hash,
                    receipt_id,
                    shares_minted,
                    received_at: Utc::now(),
                }]),
                Self::Completed { .. } | Self::TokensReceived { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },

            Finalize => match self {
                Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Err(TokenizedEquityMintError::TokensNotReceived)
                }
                Self::TokensReceived { .. } => Ok(vec![MintCompleted {
                    completed_at: Utc::now(),
                }]),
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
        }
    }
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
    /// Attempted to finalize before tokens were received
    #[error("Cannot finalize: tokens not received")]
    TokensNotReceived,
    /// Attempted to modify a completed mint operation
    #[error("Already completed")]
    AlreadyCompleted,
    /// Attempted to modify a failed mint operation
    #[error("Already failed")]
    AlreadyFailed,
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
            Self::MintCompleted { .. } => "TokenizedEquityMintEvent::MintCompleted".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
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

    fn try_apply_completed(&self, completed_at: DateTime<Utc>) -> Option<Self> {
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
            return None;
        };

        Some(Self::Completed {
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
    use st0x_event_sorcery::Aggregate;

    use super::*;
    use st0x_event_sorcery::{Lifecycle, LifecycleError};

    #[tokio::test]
    async fn test_request_mint_from_uninitialized() {
        let aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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

        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();

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
        let aggregate = Lifecycle::<TokenizedEquityMint>::default();

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
            Err(LifecycleError::Apply(
                TokenizedEquityMintError::NotRequested
            ))
        ));
    }

    #[tokio::test]
    async fn test_cannot_receive_tokens_before_acceptance() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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

        assert!(matches!(
            result,
            Err(LifecycleError::Apply(TokenizedEquityMintError::NotAccepted))
        ));
    }

    #[tokio::test]
    async fn test_cannot_finalize_before_tokens_received() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
            Err(LifecycleError::Apply(
                TokenizedEquityMintError::TokensNotReceived
            ))
        ));
    }

    #[tokio::test]
    async fn test_reject_mint_from_requested_state() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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
                TokenizedEquityMintCommand::RejectMint {
                    reason: "Cannot reject completed mint".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(LifecycleError::Apply(
                TokenizedEquityMintError::AlreadyCompleted
            ))
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_mint_when_already_failed() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let wallet = Address::random();

        let requested_event = TokenizedEquityMintEvent::MintRequested {
            symbol,
            quantity: dec!(100.5),
            wallet,
            requested_at: Utc::now(),
        };
        aggregate.apply(requested_event);

        let rejected_event = TokenizedEquityMintEvent::MintRejected {
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
            Err(LifecycleError::Apply(
                TokenizedEquityMintError::AlreadyFailed
            ))
        ));
    }

    #[tokio::test]
    async fn test_cannot_reject_mint_before_request() {
        let aggregate = Lifecycle::<TokenizedEquityMint>::default();

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
            Err(LifecycleError::Apply(
                TokenizedEquityMintError::NotRequested
            ))
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_acceptance_before_acceptance() {
        let mut aggregate = Lifecycle::<TokenizedEquityMint>::default();
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

        assert!(matches!(
            result,
            Err(LifecycleError::Apply(TokenizedEquityMintError::NotAccepted))
        ));
    }

    #[test]
    fn test_evolve_accepted_rejects_wrong_state() {
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

        let result = TokenizedEquityMint::evolve(&event, &completed).unwrap();
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

        let result = TokenizedEquityMint::evolve(&event, &requested).unwrap();
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

        let result = TokenizedEquityMint::evolve(&event, &accepted).unwrap();
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

        let result = TokenizedEquityMint::evolve(&event, &accepted).unwrap();
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

        let result = TokenizedEquityMint::evolve(&event, &requested).unwrap();
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

        let result = TokenizedEquityMint::evolve(&event, &requested).unwrap();
        assert_eq!(result, None);
    }
}
