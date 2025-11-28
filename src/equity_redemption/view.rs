use alloy::primitives::{Address, TxHash};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;
use tracing::warn;

use super::{EquityRedemption, RedemptionId};
use crate::equity_redemption::EquityRedemptionEvent;
use crate::tokenized_equity_mint::TokenizationRequestId;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EquityRedemptionView {
    NotStarted,
    TokensSent {
        redemption_id: RedemptionId,
        symbol: Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    },
    Pending {
        redemption_id: RedemptionId,
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        sent_at: DateTime<Utc>,
        detected_at: DateTime<Utc>,
    },
    Completed {
        redemption_id: RedemptionId,
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: TxHash,
        tokenization_request_id: TokenizationRequestId,
        completed_at: DateTime<Utc>,
    },
    Failed {
        redemption_id: RedemptionId,
        symbol: Symbol,
        quantity: Decimal,
        tx_hash: Option<TxHash>,
        tokenization_request_id: Option<TokenizationRequestId>,
        failure_reason: String,
        sent_at: Option<DateTime<Utc>>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for EquityRedemptionView {
    fn default() -> Self {
        Self::NotStarted
    }
}

impl View<EquityRedemption> for EquityRedemptionView {
    fn update(&mut self, event: &EventEnvelope<EquityRedemption>) {
        match &event.payload {
            EquityRedemptionEvent::TokensSent {
                symbol,
                quantity,
                redemption_wallet,
                tx_hash,
                sent_at,
            } => {
                self.handle_tokens_sent(
                    &event.aggregate_id,
                    symbol,
                    *quantity,
                    *redemption_wallet,
                    *tx_hash,
                    *sent_at,
                );
            }
            EquityRedemptionEvent::Detected {
                tokenization_request_id,
                detected_at,
            } => {
                self.handle_detected(tokenization_request_id, *detected_at);
            }
            EquityRedemptionEvent::Completed { completed_at } => {
                self.handle_completed(*completed_at);
            }
            EquityRedemptionEvent::Failed { reason, failed_at } => {
                self.handle_failed(reason, *failed_at);
            }
        }
    }
}

impl EquityRedemptionView {
    fn handle_tokens_sent(
        &mut self,
        aggregate_id: &str,
        symbol: &Symbol,
        quantity: Decimal,
        redemption_wallet: Address,
        tx_hash: TxHash,
        sent_at: DateTime<Utc>,
    ) {
        *self = Self::TokensSent {
            redemption_id: RedemptionId::new(aggregate_id),
            symbol: symbol.clone(),
            quantity,
            redemption_wallet,
            tx_hash,
            sent_at,
        };
    }

    fn handle_detected(
        &mut self,
        tokenization_request_id: &TokenizationRequestId,
        detected_at: DateTime<Utc>,
    ) {
        let Self::TokensSent {
            redemption_id,
            symbol,
            quantity,
            tx_hash,
            sent_at,
            ..
        } = self
        else {
            warn!("Received Detected event but view is not in TokensSent state: {self:?}");
            return;
        };

        *self = Self::Pending {
            redemption_id: redemption_id.clone(),
            symbol: symbol.clone(),
            quantity: *quantity,
            tx_hash: *tx_hash,
            tokenization_request_id: tokenization_request_id.clone(),
            sent_at: *sent_at,
            detected_at,
        };
    }

    fn handle_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::Pending {
            redemption_id,
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            ..
        } = self
        else {
            warn!("Received Completed event but view is not in Pending state: {self:?}");
            return;
        };

        *self = Self::Completed {
            redemption_id: redemption_id.clone(),
            symbol: symbol.clone(),
            quantity: *quantity,
            tx_hash: *tx_hash,
            tokenization_request_id: tokenization_request_id.clone(),
            completed_at,
        };
    }

    fn handle_failed(&mut self, reason: &str, failed_at: DateTime<Utc>) {
        let (redemption_id, symbol, quantity, tx_hash, tokenization_request_id, sent_at) =
            match self {
                Self::NotStarted { .. } => {
                    warn!("Received Failed event but view is in NotStarted state: {self:?}");
                    return;
                }
                Self::TokensSent {
                    redemption_id,
                    symbol,
                    quantity,
                    tx_hash,
                    sent_at,
                    ..
                } => (
                    redemption_id.clone(),
                    symbol.clone(),
                    *quantity,
                    Some(*tx_hash),
                    None,
                    Some(*sent_at),
                ),
                Self::Pending {
                    redemption_id,
                    symbol,
                    quantity,
                    tx_hash,
                    tokenization_request_id,
                    sent_at,
                    ..
                } => (
                    redemption_id.clone(),
                    symbol.clone(),
                    *quantity,
                    Some(*tx_hash),
                    Some(tokenization_request_id.clone()),
                    Some(*sent_at),
                ),
                Self::Completed { .. } | Self::Failed { .. } => {
                    warn!("Received Failed event but view is in terminal state: {self:?}");
                    return;
                }
            };

        *self = Self::Failed {
            redemption_id,
            symbol,
            quantity,
            tx_hash,
            tokenization_request_id,
            failure_reason: reason.to_string(),
            sent_at,
            failed_at,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use std::collections::HashMap;

    #[test]
    fn test_view_tracks_complete_flow() {
        let mut view = EquityRedemptionView::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let envelope = EventEnvelope {
            aggregate_id: "redemption-123".to_string(),
            sequence: 1,
            payload: EquityRedemptionEvent::TokensSent {
                symbol: symbol.clone(),
                quantity: dec!(50.25),
                redemption_wallet,
                tx_hash,
                sent_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        let EquityRedemptionView::TokensSent {
            redemption_id,
            symbol: view_symbol,
            quantity,
            tx_hash: view_tx_hash,
            ..
        } = &view
        else {
            panic!("Expected TokensSent state, got {view:?}");
        };

        assert_eq!(redemption_id, &RedemptionId::new("redemption-123"));
        assert_eq!(view_symbol, &symbol);
        assert_eq!(*quantity, dec!(50.25));
        assert_eq!(*view_tx_hash, tx_hash);

        let envelope = EventEnvelope {
            aggregate_id: "redemption-123".to_string(),
            sequence: 2,
            payload: EquityRedemptionEvent::Detected {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                detected_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        let EquityRedemptionView::Pending {
            tokenization_request_id,
            ..
        } = &view
        else {
            panic!("Expected Pending state, got {view:?}");
        };

        assert_eq!(
            tokenization_request_id,
            &TokenizationRequestId("REQ789".to_string())
        );

        let envelope = EventEnvelope {
            aggregate_id: "redemption-123".to_string(),
            sequence: 3,
            payload: EquityRedemptionEvent::Completed {
                completed_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        assert!(matches!(view, EquityRedemptionView::Completed { .. }));
    }

    #[test]
    fn test_view_captures_failure_state() {
        let mut view = EquityRedemptionView::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let redemption_wallet = Address::random();
        let tx_hash = TxHash::random();

        let envelope = EventEnvelope {
            aggregate_id: "redemption-456".to_string(),
            sequence: 1,
            payload: EquityRedemptionEvent::TokensSent {
                symbol: symbol.clone(),
                quantity: dec!(50.25),
                redemption_wallet,
                tx_hash,
                sent_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        let envelope = EventEnvelope {
            aggregate_id: "redemption-456".to_string(),
            sequence: 2,
            payload: EquityRedemptionEvent::Detected {
                tokenization_request_id: TokenizationRequestId("REQ789".to_string()),
                detected_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        let envelope = EventEnvelope {
            aggregate_id: "redemption-456".to_string(),
            sequence: 3,
            payload: EquityRedemptionEvent::Failed {
                reason: "Redemption rejected".to_string(),
                failed_at: Utc::now(),
            },
            metadata: HashMap::default(),
        };
        view.update(&envelope);

        let EquityRedemptionView::Failed {
            redemption_id,
            symbol: view_symbol,
            quantity,
            tx_hash: view_tx_hash,
            tokenization_request_id,
            failure_reason,
            sent_at,
            ..
        } = &view
        else {
            panic!("Expected Failed state, got {view:?}");
        };

        assert_eq!(redemption_id, &RedemptionId::new("redemption-456"));
        assert_eq!(view_symbol, &symbol);
        assert_eq!(*quantity, dec!(50.25));
        assert_eq!(*view_tx_hash, Some(tx_hash));
        assert_eq!(
            *tokenization_request_id,
            Some(TokenizationRequestId("REQ789".to_string()))
        );
        assert_eq!(failure_reason, "Redemption rejected");
        assert!(sent_at.is_some());
    }
}
