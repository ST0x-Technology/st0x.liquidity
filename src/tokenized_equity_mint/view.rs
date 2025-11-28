use alloy::primitives::{Address, TxHash, U256};
use chrono::{DateTime, Utc};
use cqrs_es::{EventEnvelope, View};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use st0x_broker::Symbol;
use tracing::warn;

use super::{IssuerRequestId, MintId, ReceiptId, TokenizationRequestId, TokenizedEquityMint};
use crate::tokenized_equity_mint::TokenizedEquityMintEvent;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum TokenizedEquityMintView {
    NotStarted,
    Requested {
        mint_id: MintId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    },
    Accepted {
        mint_id: MintId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        issuer_request_id: IssuerRequestId,
        tokenization_request_id: TokenizationRequestId,
        requested_at: DateTime<Utc>,
        accepted_at: DateTime<Utc>,
    },
    TokensReceived {
        mint_id: MintId,
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
        mint_id: MintId,
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
        mint_id: MintId,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
        failure_reason: String,
        requested_at: DateTime<Utc>,
        failed_at: DateTime<Utc>,
    },
}

impl Default for TokenizedEquityMintView {
    fn default() -> Self {
        Self::NotStarted
    }
}

impl View<TokenizedEquityMint> for TokenizedEquityMintView {
    fn update(&mut self, event: &EventEnvelope<TokenizedEquityMint>) {
        match &event.payload {
            TokenizedEquityMintEvent::MintRequested {
                symbol,
                quantity,
                wallet,
                requested_at,
            } => {
                self.handle_mint_requested(
                    &event.aggregate_id,
                    symbol,
                    *quantity,
                    *wallet,
                    *requested_at,
                );
            }
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id,
                tokenization_request_id,
                accepted_at,
            } => {
                self.handle_mint_accepted(issuer_request_id, tokenization_request_id, *accepted_at);
            }
            TokenizedEquityMintEvent::TokensReceived {
                tx_hash,
                receipt_id,
                shares_minted,
                received_at,
            } => {
                self.handle_tokens_received(*tx_hash, receipt_id, *shares_minted, *received_at);
            }
            TokenizedEquityMintEvent::MintCompleted { completed_at } => {
                self.handle_mint_completed(*completed_at);
            }
            TokenizedEquityMintEvent::MintFailed { reason, failed_at } => {
                self.handle_mint_failed(reason, *failed_at);
            }
        }
    }
}

impl TokenizedEquityMintView {
    fn handle_mint_requested(
        &mut self,
        aggregate_id: &str,
        symbol: &Symbol,
        quantity: Decimal,
        wallet: Address,
        requested_at: DateTime<Utc>,
    ) {
        *self = Self::Requested {
            mint_id: MintId::new(aggregate_id),
            symbol: symbol.clone(),
            quantity,
            wallet,
            requested_at,
        };
    }

    fn handle_mint_accepted(
        &mut self,
        issuer_request_id: &IssuerRequestId,
        tokenization_request_id: &TokenizationRequestId,
        accepted_at: DateTime<Utc>,
    ) {
        let Self::Requested {
            mint_id,
            symbol,
            quantity,
            wallet,
            requested_at,
        } = self
        else {
            warn!("Received MintAccepted event but view is not in Requested state: {self:?}");
            return;
        };

        *self = Self::Accepted {
            mint_id: mint_id.clone(),
            symbol: symbol.clone(),
            quantity: *quantity,
            wallet: *wallet,
            issuer_request_id: issuer_request_id.clone(),
            tokenization_request_id: tokenization_request_id.clone(),
            requested_at: *requested_at,
            accepted_at,
        };
    }

    fn handle_tokens_received(
        &mut self,
        tx_hash: TxHash,
        receipt_id: &ReceiptId,
        shares_minted: U256,
        received_at: DateTime<Utc>,
    ) {
        let Self::Accepted {
            mint_id,
            symbol,
            quantity,
            wallet,
            issuer_request_id,
            tokenization_request_id,
            requested_at,
            accepted_at,
        } = self
        else {
            warn!("Received TokensReceived event but view is not in Accepted state: {self:?}");
            return;
        };

        *self = Self::TokensReceived {
            mint_id: mint_id.clone(),
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
        };
    }

    fn handle_mint_completed(&mut self, completed_at: DateTime<Utc>) {
        let Self::TokensReceived {
            mint_id,
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
            warn!("Received MintCompleted event but view is not in TokensReceived state: {self:?}");
            return;
        };

        *self = Self::Completed {
            mint_id: mint_id.clone(),
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

    fn handle_mint_failed(&mut self, reason: &str, failed_at: DateTime<Utc>) {
        match self {
            Self::Requested {
                mint_id,
                symbol,
                quantity,
                wallet,
                requested_at,
            }
            | Self::Accepted {
                mint_id,
                symbol,
                quantity,
                wallet,
                requested_at,
                ..
            }
            | Self::TokensReceived {
                mint_id,
                symbol,
                quantity,
                wallet,
                requested_at,
                ..
            } => {
                *self = Self::Failed {
                    mint_id: mint_id.clone(),
                    symbol: symbol.clone(),
                    quantity: *quantity,
                    wallet: *wallet,
                    failure_reason: reason.to_string(),
                    requested_at: *requested_at,
                    failed_at,
                };
            }
            _ => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use alloy::primitives::U256;
    use rust_decimal_macros::dec;

    fn test_symbol() -> Symbol {
        Symbol::new("AAPL").unwrap()
    }

    fn test_wallet() -> Address {
        Address::random()
    }

    fn create_event_envelope(
        aggregate_id: &str,
        sequence: usize,
        payload: TokenizedEquityMintEvent,
    ) -> EventEnvelope<TokenizedEquityMint> {
        EventEnvelope {
            aggregate_id: aggregate_id.to_string(),
            sequence,
            payload,
            metadata: HashMap::default(),
        }
    }

    #[test]
    fn test_view_tracks_complete_flow() {
        let mut view = TokenizedEquityMintView::default();
        let symbol = test_symbol();
        let wallet = test_wallet();
        let tx_hash = TxHash::random();

        let event1 = create_event_envelope(
            "mint-123",
            1,
            TokenizedEquityMintEvent::MintRequested {
                symbol: symbol.clone(),
                quantity: dec!(100.5),
                wallet,
                requested_at: Utc::now(),
            },
        );
        view.update(&event1);

        let TokenizedEquityMintView::Requested {
            mint_id,
            symbol: view_symbol,
            quantity,
            wallet: view_wallet,
            requested_at,
        } = &view
        else {
            panic!("Expected Requested state, got {view:?}");
        };

        assert_eq!(mint_id, &MintId::new("mint-123"));
        assert_eq!(view_symbol, &symbol);
        assert_eq!(quantity, &dec!(100.5));
        assert_eq!(view_wallet, &wallet);
        assert!(requested_at <= &Utc::now());

        let event2 = create_event_envelope(
            "mint-123",
            2,
            TokenizedEquityMintEvent::MintAccepted {
                issuer_request_id: IssuerRequestId("ISS123".to_string()),
                tokenization_request_id: TokenizationRequestId("TOK456".to_string()),
                accepted_at: Utc::now(),
            },
        );
        view.update(&event2);

        let TokenizedEquityMintView::Accepted {
            issuer_request_id,
            tokenization_request_id,
            ..
        } = &view
        else {
            panic!("Expected Accepted state, got {view:?}");
        };

        assert_eq!(issuer_request_id, &IssuerRequestId("ISS123".to_string()));
        assert_eq!(
            tokenization_request_id,
            &TokenizationRequestId("TOK456".to_string())
        );

        let event3 = create_event_envelope(
            "mint-123",
            3,
            TokenizedEquityMintEvent::TokensReceived {
                tx_hash,
                receipt_id: ReceiptId(U256::from(789)),
                shares_minted: U256::from(100_500_000_000_000_000_000_u128),
                received_at: Utc::now(),
            },
        );
        view.update(&event3);

        let TokenizedEquityMintView::TokensReceived {
            tx_hash: view_tx_hash,
            receipt_id,
            shares_minted,
            ..
        } = &view
        else {
            panic!("Expected TokensReceived state, got {view:?}");
        };

        assert_eq!(view_tx_hash, &tx_hash);
        assert_eq!(receipt_id, &ReceiptId(U256::from(789)));
        assert_eq!(shares_minted, &U256::from(100_500_000_000_000_000_000_u128));

        let event4 = create_event_envelope(
            "mint-123",
            4,
            TokenizedEquityMintEvent::MintCompleted {
                completed_at: Utc::now(),
            },
        );
        view.update(&event4);

        let TokenizedEquityMintView::Completed { completed_at, .. } = &view else {
            panic!("Expected Completed state, got {view:?}");
        };

        assert!(completed_at <= &Utc::now());
    }

    #[test]
    fn test_view_captures_failure_state() {
        let mut view = TokenizedEquityMintView::default();
        let symbol = test_symbol();
        let wallet = test_wallet();

        let event1 = create_event_envelope(
            "mint-456",
            1,
            TokenizedEquityMintEvent::MintRequested {
                symbol,
                quantity: dec!(50.25),
                wallet,
                requested_at: Utc::now(),
            },
        );
        view.update(&event1);

        let TokenizedEquityMintView::Requested { mint_id, .. } = &view else {
            panic!("Expected Requested state, got {view:?}");
        };

        assert_eq!(mint_id, &MintId::new("mint-456"));

        let event2 = create_event_envelope(
            "mint-456",
            2,
            TokenizedEquityMintEvent::MintFailed {
                reason: "Alpaca API timeout".to_string(),
                failed_at: Utc::now(),
            },
        );
        view.update(&event2);

        let TokenizedEquityMintView::Failed {
            failure_reason,
            failed_at,
            ..
        } = &view
        else {
            panic!("Expected Failed state, got {view:?}");
        };

        assert_eq!(failure_reason, "Alpaca API timeout");
        assert!(failed_at <= &Utc::now());
    }
}
