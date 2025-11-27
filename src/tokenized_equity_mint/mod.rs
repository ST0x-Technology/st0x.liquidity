use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

mod cmd;
mod event;
mod view;

pub(crate) use cmd::TokenizedEquityMintCommand;
pub(crate) use event::TokenizedEquityMintEvent;
pub(crate) use view::TokenizedEquityMintView;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct IssuerRequestId(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TokenizationRequestId(pub(crate) String);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ReceiptId(pub(crate) U256);

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum TokenizedEquityMintError {
    #[error("Cannot accept mint: not in requested state")]
    NotRequested,
    #[error("Cannot receive tokens: mint not accepted")]
    NotAccepted,
    #[error("Cannot finalize: tokens not received")]
    TokensNotReceived,
    #[error("Already completed")]
    AlreadyCompleted,
    #[error("Already failed")]
    AlreadyFailed,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub(crate) enum TokenizedEquityMint {
    NotStarted,
    MintRequested {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    MintAccepted {
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },
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
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for TokenizedEquityMint {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[async_trait]
impl Aggregate for TokenizedEquityMint {
    type Command = TokenizedEquityMintCommand;
    type Event = TokenizedEquityMintEvent;
    type Error = TokenizedEquityMintError;
    type Services = ();

    fn aggregate_type() -> String {
        "TokenizedEquityMint".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            TokenizedEquityMintCommand::RequestMint {
                symbol,
                quantity,
                wallet,
            } => match self {
                Self::NotStarted => {
                    let now = Utc::now();

                    Ok(vec![TokenizedEquityMintEvent::MintRequested {
                        symbol,
                        quantity,
                        wallet,
                        requested_at: now,
                    }])
                }
                Self::MintRequested { .. }
                | Self::MintAccepted { .. }
                | Self::TokensReceived { .. }
                | Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
            TokenizedEquityMintCommand::AcknowledgeAcceptance {
                issuer_request_id,
                tokenization_request_id,
            } => match self {
                Self::MintRequested { .. } => {
                    let now = Utc::now();

                    Ok(vec![TokenizedEquityMintEvent::MintAccepted {
                        issuer_request_id,
                        tokenization_request_id,
                        accepted_at: now,
                    }])
                }
                Self::NotStarted => Err(TokenizedEquityMintError::NotRequested),
                Self::MintAccepted { .. }
                | Self::TokensReceived { .. }
                | Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
            TokenizedEquityMintCommand::ReceiveTokens {
                tx_hash,
                receipt_id,
                shares_minted,
            } => match self {
                Self::MintAccepted { .. } => {
                    let now = Utc::now();

                    Ok(vec![TokenizedEquityMintEvent::TokensReceived {
                        tx_hash,
                        receipt_id,
                        shares_minted,
                        received_at: now,
                    }])
                }
                Self::NotStarted | Self::MintRequested { .. } => {
                    Err(TokenizedEquityMintError::NotAccepted)
                }
                Self::TokensReceived { .. } | Self::Completed { .. } => {
                    Err(TokenizedEquityMintError::AlreadyCompleted)
                }
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
            TokenizedEquityMintCommand::Finalize => match self {
                Self::TokensReceived { .. } => {
                    let now = Utc::now();

                    Ok(vec![TokenizedEquityMintEvent::MintCompleted {
                        completed_at: now,
                    }])
                }
                Self::NotStarted | Self::MintRequested { .. } | Self::MintAccepted { .. } => {
                    Err(TokenizedEquityMintError::TokensNotReceived)
                }
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
            TokenizedEquityMintCommand::Fail { reason } => match self {
                Self::NotStarted => Err(TokenizedEquityMintError::NotRequested),
                Self::MintRequested { .. }
                | Self::MintAccepted { .. }
                | Self::TokensReceived { .. } => {
                    let now = Utc::now();

                    Ok(vec![TokenizedEquityMintEvent::MintFailed {
                        reason,
                        failed_at: now,
                    }])
                }
                Self::Completed { .. } => Err(TokenizedEquityMintError::AlreadyCompleted),
                Self::Failed { .. } => Err(TokenizedEquityMintError::AlreadyFailed),
            },
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            TokenizedEquityMintEvent::MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => {
                self.apply_mint_requested(symbol, quantity, wallet, requested_at);
            }
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => {
                self.apply_mint_accepted(issuer_request_id, tokenization_request_id, accepted_at);
            }
            TokenizedEquityMintEvent::TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => {
                self.apply_tokens_received(tx_hash, receipt_id, shares_minted, received_at);
            }
            TokenizedEquityMintEvent::MintCompleted { completed_at } => {
                self.apply_mint_completed(completed_at);
            }
            TokenizedEquityMintEvent::MintFailed { reason, failed_at } => {
                self.apply_mint_failed(reason, failed_at);
            }
        }
    }
}

impl TokenizedEquityMint {
    fn apply_mint_requested(
        &mut self,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    ) {
        *self = Self::MintRequested {
            symbol,
            quantity,
            wallet,
            requested_at,
        };
    }

    fn apply_mint_accepted(
        &mut self,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    ) {
        if let Self::MintRequested {
            symbol,
            quantity,
            wallet,
            requested_at,
        } = self
        {
            *self = Self::MintAccepted {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                issuer_request_id,
                tokenization_request_id,
                requested_at: *requested_at,
                accepted_at,
            };
        }
    }

    fn apply_tokens_received(
        &mut self,
        tx_hash: TxHash,
        receipt_id: ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    ) {
        if let Self::MintAccepted {
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            requested_at,
            accepted_at,
        } = self
        {
            *self = Self::TokensReceived {
                symbol: symbol.clone(),
                quantity: *quantity,
                wallet: *wallet,
                issuer_request_id: issuer_request_id.clone(),
                tokenization_request_id: tokenization_request_id.clone(),
                tx_hash,
                receipt_id,
                shares_minted,
                requested_at: *requested_at,
                accepted_at: *accepted_at,
                received_at,
            };
        }
    }

    fn apply_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        if let Self::TokensReceived {
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
        {
            *self = Self::Completed {
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
            };
        }
    }

    fn apply_mint_failed(&mut self, reason: String, failed_at: DateTime<Utc>) {
        let (symbol, quantity, requested_at) = match self {
            Self::MintRequested {
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
            } => (symbol.clone(), *quantity, *requested_at),
            _ => return,
        };

        *self = Self::Failed {
            symbol,
            quantity,
            reason,
            requested_at,
            failed_at,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn test_wallet() -> Address {
        Address::random()
    }

    #[tokio::test]
    async fn test_request_mint_from_not_started() {
        let aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();
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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();
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
        let symbol = test_symbol();
        let wallet = test_wallet();
        let tx_hash = TxHash::random();

        let mut aggregate = TokenizedEquityMint::default();

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

        assert!(matches!(aggregate, TokenizedEquityMint::Completed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_acknowledge_before_request() {
        let aggregate = TokenizedEquityMint::default();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();
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
        let mut aggregate = TokenizedEquityMint::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

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
}
