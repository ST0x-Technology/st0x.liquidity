use alloy::primitives::{Address, TxHash};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use cqrs_es::Aggregate;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;

use crate::tokenized_equity_mint::TokenizationRequestId;

mod cmd;
mod event;

pub(crate) use cmd::EquityRedemptionCommand;
pub(crate) use event::EquityRedemptionEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EquityRedemption {
    NotStarted,
    TokensSent {
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    },
    Pending {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },
    Completed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        completed_at: DateTime<Utc>,
    },
    Failed {
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        reason: String,
        sent_at: Option<DateTime<Utc>>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for EquityRedemption {
    fn default() -> Self {
        Self::NotStarted
    }
}

#[derive(Debug, thiserror::Error, PartialEq)]
pub(crate) enum EquityRedemptionError {
    #[error("Cannot detect redemption: tokens not sent")]
    TokensNotSent,

    #[error("Cannot complete: not in pending state")]
    NotPending,

    #[error("Already completed")]
    AlreadyCompleted,

    #[error("Already failed")]
    AlreadyFailed,
}

#[async_trait]
impl Aggregate for EquityRedemption {
    type Command = EquityRedemptionCommand;
    type Event = EquityRedemptionEvent;
    type Error = EquityRedemptionError;
    type Services = ();

    fn aggregate_type() -> String {
        "EquityRedemption".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match (self, command) {
            (
                Self::NotStarted,
                EquityRedemptionCommand::SendTokens {
                    symbol,
                    quantity,
                    redemption_wallet,
                    tx_hash,
                },
            ) => Ok(vec![EquityRedemptionEvent::TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at: Utc::now(),
            }]),

            (
                Self::TokensSent { .. },
                EquityRedemptionCommand::Detect {
                    tokenization_request_id,
                },
            ) => Ok(vec![EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at: Utc::now(),
            }]),

            (Self::Pending { .. }, EquityRedemptionCommand::Complete) => {
                Ok(vec![EquityRedemptionEvent::Completed {
                    completed_at: Utc::now(),
                }])
            }

            (
                Self::NotStarted | Self::TokensSent { .. } | Self::Pending { .. },
                EquityRedemptionCommand::Fail { reason },
            ) => Ok(vec![EquityRedemptionEvent::Failed {
                reason,
                failed_at: Utc::now(),
            }]),

            (Self::NotStarted, EquityRedemptionCommand::Detect { .. }) => {
                Err(EquityRedemptionError::TokensNotSent)
            }

            (Self::NotStarted | Self::TokensSent { .. }, EquityRedemptionCommand::Complete) => {
                Err(EquityRedemptionError::NotPending)
            }

            (
                Self::TokensSent { .. } | Self::Pending { .. },
                EquityRedemptionCommand::SendTokens { .. },
            )
            | (Self::Pending { .. }, EquityRedemptionCommand::Detect { .. })
            | (Self::Completed { .. }, EquityRedemptionCommand::Fail { .. }) => {
                Err(EquityRedemptionError::AlreadyCompleted)
            }

            (Self::Failed { .. }, EquityRedemptionCommand::Fail { .. }) => {
                Err(EquityRedemptionError::AlreadyFailed)
            }

            (Self::Completed { .. }, _) => Err(EquityRedemptionError::AlreadyCompleted),

            (Self::Failed { .. }, _) => Err(EquityRedemptionError::AlreadyFailed),
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            EquityRedemptionEvent::TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at,
            } => {
                self.apply_tokens_sent(symbol, quantity, redemption_wallet, tx_hash, sent_at);
            }
            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => {
                self.apply_detected(tokenization_request_id, detected_at);
            }
            EquityRedemptionEvent::Completed { completed_at } => {
                self.apply_completed(completed_at);
            }
            EquityRedemptionEvent::Failed { reason, failed_at } => {
                self.apply_failed(reason, failed_at);
            }
        }
    }
}

impl EquityRedemption {
    fn apply_tokens_sent(
        &mut self,
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    ) {
        *self = Self::TokensSent {
            symbol,
            quantity,
            redemption_wallet,
            tx_hash,
            sent_at,
        };
    }

    fn apply_detected(
        &mut self,
        tokenization_request_id: TokenizationRequestId,
        detected_at: DateTime<Utc>,
    ) {
        if let Self::TokensSent {
            symbol,
            quantity,
            tx_hash,
            sent_at,
            ..
        } = self
        {
            *self = Self::Pending {
                symbol: symbol.clone(),
                quantity: *quantity,
                tx_hash: *tx_hash,
                tokenization_request_id,
                sent_at: *sent_at,
                detected_at,
            };
        }
    }

    fn apply_completed(&mut self, completed_at: DateTime<Utc>) {
        if let Self::Pending {
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            ..
        } = self
        {
            *self = Self::Completed {
                symbol: symbol.clone(),
                quantity: *quantity,
                tx_hash: *tx_hash,
                tokenization_request_id: tokenization_request_id.clone(),
                completed_at,
            };
        }
    }

    fn apply_failed(&mut self, reason: String, failed_at: DateTime<Utc>) {
        let (symbol, quantity, tx_hash, tokenization_request_id, sent_at) = match self {
            Self::TokensSent {
                symbol,
                quantity,
                tx_hash,
                sent_at,
                ..
            } => (
                symbol.clone(),
                *quantity,
                Some(*tx_hash),
                None,
                Some(*sent_at),
            ),
            Self::Pending {
                symbol,
                quantity,
                tx_hash,
                tokenization_request_id,
                sent_at,
                ..
            } => (
                symbol.clone(),
                *quantity,
                Some(*tx_hash),
                Some(tokenization_request_id.clone()),
                Some(*sent_at),
            ),
            Self::NotStarted | Self::Completed { .. } | Self::Failed { .. } => {
                return;
            }
        };

        *self = Self::Failed {
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            reason,
            sent_at,
            failed_at,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[tokio::test]
    async fn test_send_tokens_from_not_started() {
        let aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let events = aggregate
            .handle(
                EquityRedemptionCommand::SendTokens {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    redemption_wallet,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(
            events[0],
            EquityRedemptionEvent::TokensSent { .. }
        ));
    }

    #[tokio::test]
    async fn test_detect_after_tokens_sent() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Detected { .. }));
    }

    #[tokio::test]
    async fn test_complete_from_pending() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Completed { .. }));
    }

    #[tokio::test]
    async fn test_complete_redemption_flow_end_to_end() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let send_events = aggregate
            .handle(
                EquityRedemptionCommand::SendTokens {
                    symbol: symbol.clone(),
                    quantity: dec!(50.25),
                    redemption_wallet,
                    tx_hash,
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(send_events.len(), 1);
        aggregate.apply(send_events[0].clone());

        let detect_events = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &(),
            )
            .await
            .unwrap();
        assert_eq!(detect_events.len(), 1);
        aggregate.apply(detect_events[0].clone());

        let complete_events = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
            .await
            .unwrap();
        assert_eq!(complete_events.len(), 1);

        assert!(matches!(aggregate, EquityRedemption::Pending { .. }));
    }

    #[tokio::test]
    async fn test_cannot_detect_before_sending_tokens() {
        let aggregate = EquityRedemption::default();

        let result = aggregate
            .handle(
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::TokensNotSent)));
    }

    #[tokio::test]
    async fn test_cannot_complete_before_pending() {
        let aggregate = EquityRedemption::default();

        let result = aggregate
            .handle(EquityRedemptionCommand::Complete, &())
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::NotPending)));
    }

    #[tokio::test]
    async fn test_fail_from_tokens_sent_state() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Fail {
                    reason: "Redemption rejected".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Failed { .. }));
    }

    #[tokio::test]
    async fn test_fail_from_pending_state() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let events = aggregate
            .handle(
                EquityRedemptionCommand::Fail {
                    reason: "Redemption failed".to_string(),
                },
                &(),
            )
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], EquityRedemptionEvent::Failed { .. }));
    }

    #[tokio::test]
    async fn test_cannot_fail_when_completed() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let completed_event = EquityRedemptionEvent::Completed {
            completed_at: Utc::now(),
        };
        aggregate.apply(completed_event);

        let result = aggregate
            .handle(
                EquityRedemptionCommand::Fail {
                    reason: "Cannot fail".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(
            result,
            Err(EquityRedemptionError::AlreadyCompleted)
        ));
    }

    #[tokio::test]
    async fn test_cannot_fail_when_already_failed() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol,
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let failed_event = EquityRedemptionEvent::Failed {
            reason: "First failure".to_string(),
            failed_at: Utc::now(),
        };
        aggregate.apply(failed_event);

        let result = aggregate
            .handle(
                EquityRedemptionCommand::Fail {
                    reason: "Second failure".to_string(),
                },
                &(),
            )
            .await;

        assert!(matches!(result, Err(EquityRedemptionError::AlreadyFailed)));
    }

    #[tokio::test]
    async fn test_failed_state_preserves_optional_context() {
        let mut aggregate = EquityRedemption::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let sent_event = EquityRedemptionEvent::TokensSent {
            symbol: symbol.clone(),
            quantity: dec!(50.25),
            redemption_wallet,
            tx_hash,
            sent_at: Utc::now(),
        };
        aggregate.apply(sent_event);

        let detected_event = EquityRedemptionEvent::Detected {
            tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
            detected_at: Utc::now(),
        };
        aggregate.apply(detected_event);

        let failed_event = EquityRedemptionEvent::Failed {
            reason: "Redemption failed".to_string(),
            failed_at: Utc::now(),
        };
        aggregate.apply(failed_event);

        let EquityRedemption::Failed {
            symbol: failed_symbol,
            quantity,
            tx_hash: failed_tx_hash,
            tokenization_request_id,
            sent_at,
            ..
        } = aggregate
        else {
            panic!("Expected Failed state, got {aggregate:?}");
        };

        assert_eq!(failed_symbol, symbol);
        assert_eq!(quantity, dec!(50.25));
        assert_eq!(failed_tx_hash, Some(tx_hash));
        assert_eq!(
            tokenization_request_id,
            Some(TokenizationRequestId("REQ789".to_string()))
        );
        assert!(sent_at.is_some());
    }
}
