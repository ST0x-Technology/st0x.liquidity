//! Orchestrates Alpaca-to-Raindex equity inventory transfer.
//!
//! Executes `Mint` and `Deposit` commands on the `TokenizedEquityMint` aggregate.
//! The aggregate handles all service interactions internally.

use alloy::primitives::Address;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{info, instrument};

use super::{Mint, MintError};
use crate::lifecycle::{Lifecycle, Never};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
};

pub(crate) struct MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
}

impl<ES> MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    pub(crate) fn new(cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>) -> Self {
        Self { cqrs }
    }

    /// Executes the full mint workflow via Mint, Wrap, and Deposit commands.
    ///
    /// The aggregate handles all service interactions internally:
    /// - Mint command: requests tokenization, polls until tokens arrive
    /// - Wrap command: wraps unwrapped tokens into ERC-4626 vault shares
    /// - Deposit command: deposits wrapped tokens to Raindex
    #[instrument(skip(self), fields(%symbol, ?quantity, %wallet))]
    async fn execute_mint_impl(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        info!(%symbol, ?quantity, %wallet, "Starting mint workflow");

        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::Mint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        info!("Mint command completed, executing Wrap command");

        self.cqrs
            .execute(&issuer_request_id.0, TokenizedEquityMintCommand::Wrap)
            .await?;

        info!("Wrap command completed, executing Deposit command");

        self.cqrs
            .execute(&issuer_request_id.0, TokenizedEquityMintCommand::Deposit)
            .await?;

        info!("Mint workflow completed successfully");
        Ok(())
    }
}

#[async_trait]
impl<ES> Mint for MintManager<ES>
where
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>> + Send + Sync,
    ES::AC: Send,
{
    async fn execute_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        symbol: Symbol,
        quantity: FractionalShares,
        wallet: Address,
    ) -> Result<(), MintError> {
        self.execute_mint_impl(issuer_request_id, symbol, quantity, wallet)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use rust_decimal_macros::dec;
    use std::sync::Arc;

    use super::*;
    use crate::onchain::mock::MockVault;
    use crate::tokenization::TokenizationRequestStatus;
    use crate::tokenization::mock::{MockMintPollOutcome, MockMintRequestOutcome, MockTokenizer};
    use crate::tokenized_equity_mint::{
        HttpStatusCode, MintServices, TokenizedEquityMintError, TokenizedEquityMintEvent,
    };
    use crate::wrapper::mock::MockWrapper;

    fn mock_mint_services() -> MintServices {
        MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
            raindex: Arc::new(MockVault::new()),
        }
    }

    #[tokio::test]
    async fn execute_mint_reaches_completed_state() {
        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(
            store.clone(),
            vec![],
            mock_mint_services(),
        ));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("test-001");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap();

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 6, "Expected 6 events, got: {}", events.len());

        let completed = events.last().unwrap();
        let TokenizedEquityMintEvent::Completed {
            symbol: completed_symbol,
            quantity: completed_quantity,
            ..
        } = &completed.payload
        else {
            panic!("Expected Completed event, got: {:?}", completed.payload);
        };

        assert_eq!(*completed_symbol, symbol);
        assert_eq!(*completed_quantity, quantity.inner());
    }

    #[tokio::test]
    async fn execute_mint_emits_events_in_correct_order() {
        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(
            store.clone(),
            vec![],
            mock_mint_services(),
        ));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("order-test");

        manager
            .execute_mint(
                &issuer_request_id,
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
            )
            .await
            .unwrap();

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert!(
            matches!(
                events[0].payload,
                TokenizedEquityMintEvent::MintRequested { .. }
            ),
            "First event should be MintRequested, got: {:?}",
            events[0].payload
        );
        assert!(
            matches!(
                events[1].payload,
                TokenizedEquityMintEvent::MintAccepted { .. }
            ),
            "Second event should be MintAccepted, got: {:?}",
            events[1].payload
        );
        assert!(
            matches!(
                events[2].payload,
                TokenizedEquityMintEvent::TokensReceived { .. }
            ),
            "Third event should be TokensReceived, got: {:?}",
            events[2].payload
        );
        assert!(
            matches!(
                events[3].payload,
                TokenizedEquityMintEvent::TokensWrapped { .. }
            ),
            "Fourth event should be TokensWrapped, got: {:?}",
            events[3].payload
        );
        assert!(
            matches!(
                events[4].payload,
                TokenizedEquityMintEvent::DepositedIntoRaindex { .. }
            ),
            "Fifth event should be DepositedIntoRaindex, got: {:?}",
            events[4].payload
        );
        assert!(
            matches!(
                events[5].payload,
                TokenizedEquityMintEvent::Completed { .. }
            ),
            "Sixth event should be Completed, got: {:?}",
            events[5].payload
        );
    }

    #[tokio::test]
    async fn tokenizer_rejects_mint_request_emits_mint_rejected() {
        let services = MintServices {
            tokenizer: Arc::new(
                MockTokenizer::new().with_mint_request_outcome(MockMintRequestOutcome::Rejected),
            ),
            wrapper: Arc::new(MockWrapper::new()),
            raindex: Arc::new(MockVault::new()),
        };

        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(store.clone(), vec![], services));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("rejected-test");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let err = manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                MintError::Aggregate(cqrs_es::AggregateError::UserError(
                    TokenizedEquityMintError::AlreadyFailed
                ))
            ),
            "Expected AlreadyFailed error from Wrap command after mint rejection, got: {err:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 2, "Expected 2 events, got: {}", events.len());

        assert!(
            matches!(
                events[0].payload,
                TokenizedEquityMintEvent::MintRequested { .. }
            ),
            "First event should be MintRequested, got: {:?}",
            events[0].payload
        );

        let TokenizedEquityMintEvent::MintRejected {
            symbol: rejected_symbol,
            quantity: rejected_quantity,
            status_code,
            ..
        } = &events[1].payload
        else {
            panic!(
                "Second event should be MintRejected, got: {:?}",
                events[1].payload
            );
        };

        assert_eq!(*rejected_symbol, symbol);
        assert_eq!(*rejected_quantity, quantity.inner());
        assert_eq!(*status_code, Some(HttpStatusCode(400)));
    }

    #[tokio::test]
    async fn tokenizer_poll_returns_rejected_status_emits_acceptance_failed() {
        let services = MintServices {
            tokenizer: Arc::new(
                MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Rejected),
            ),
            wrapper: Arc::new(MockWrapper::new()),
            raindex: Arc::new(MockVault::new()),
        };

        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(store.clone(), vec![], services));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("poll-rejected-test");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let err = manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                MintError::Aggregate(cqrs_es::AggregateError::UserError(
                    TokenizedEquityMintError::AlreadyFailed
                ))
            ),
            "Expected AlreadyFailed error from Wrap command after poll rejection, got: {err:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 3, "Expected 3 events, got: {}", events.len());

        assert!(
            matches!(
                events[0].payload,
                TokenizedEquityMintEvent::MintRequested { .. }
            ),
            "First event should be MintRequested, got: {:?}",
            events[0].payload
        );

        assert!(
            matches!(
                events[1].payload,
                TokenizedEquityMintEvent::MintAccepted { .. }
            ),
            "Second event should be MintAccepted, got: {:?}",
            events[1].payload
        );

        let TokenizedEquityMintEvent::MintAcceptanceFailed {
            symbol: failed_symbol,
            quantity: failed_quantity,
            last_status,
            ..
        } = &events[2].payload
        else {
            panic!(
                "Third event should be MintAcceptanceFailed, got: {:?}",
                events[2].payload
            );
        };

        assert_eq!(*failed_symbol, symbol);
        assert_eq!(*failed_quantity, quantity.inner());
        assert_eq!(*last_status, TokenizationRequestStatus::Rejected);
    }

    #[tokio::test]
    async fn tokenizer_poll_error_emits_acceptance_failed() {
        let services = MintServices {
            tokenizer: Arc::new(
                MockTokenizer::new().with_mint_poll_outcome(MockMintPollOutcome::Error),
            ),
            wrapper: Arc::new(MockWrapper::new()),
            raindex: Arc::new(MockVault::new()),
        };

        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(store.clone(), vec![], services));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("poll-error-test");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let err = manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                MintError::Aggregate(cqrs_es::AggregateError::UserError(
                    TokenizedEquityMintError::AlreadyFailed
                ))
            ),
            "Expected AlreadyFailed error from Wrap command after poll error, got: {err:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 3, "Expected 3 events, got: {}", events.len());

        let TokenizedEquityMintEvent::MintAcceptanceFailed {
            symbol: failed_symbol,
            quantity: failed_quantity,
            last_status,
            ..
        } = &events[2].payload
        else {
            panic!(
                "Third event should be MintAcceptanceFailed, got: {:?}",
                events[2].payload
            );
        };

        assert_eq!(*failed_symbol, symbol);
        assert_eq!(*failed_quantity, quantity.inner());
        assert_eq!(*last_status, TokenizationRequestStatus::Pending);
    }

    #[tokio::test]
    async fn wrapper_failure_emits_wrapping_failed() {
        let services = MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::failing()),
            raindex: Arc::new(MockVault::new()),
        };

        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(store.clone(), vec![], services));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("wrap-fail-test");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let err = manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap_err();

        assert!(
            matches!(
                err,
                MintError::Aggregate(cqrs_es::AggregateError::UserError(
                    TokenizedEquityMintError::AlreadyFailed
                ))
            ),
            "Expected AlreadyFailed error from Deposit command after wrapping failure, got: {err:?}"
        );

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 4, "Expected 4 events, got: {}", events.len());

        assert!(
            matches!(
                events[0].payload,
                TokenizedEquityMintEvent::MintRequested { .. }
            ),
            "First event should be MintRequested, got: {:?}",
            events[0].payload
        );

        assert!(
            matches!(
                events[1].payload,
                TokenizedEquityMintEvent::MintAccepted { .. }
            ),
            "Second event should be MintAccepted, got: {:?}",
            events[1].payload
        );

        assert!(
            matches!(
                events[2].payload,
                TokenizedEquityMintEvent::TokensReceived { .. }
            ),
            "Third event should be TokensReceived, got: {:?}",
            events[2].payload
        );

        let TokenizedEquityMintEvent::WrappingFailed {
            symbol: failed_symbol,
            quantity: failed_quantity,
            ..
        } = &events[3].payload
        else {
            panic!(
                "Fourth event should be WrappingFailed, got: {:?}",
                events[3].payload
            );
        };

        assert_eq!(*failed_symbol, symbol);
        assert_eq!(*failed_quantity, quantity.inner());
    }

    #[tokio::test]
    async fn raindex_deposit_failure_emits_deposit_failed() {
        let services = MintServices {
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
            raindex: Arc::new(MockVault::failing_deposit()),
        };

        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(store.clone(), vec![], services));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("deposit-fail-test");
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap();

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        assert_eq!(events.len(), 5, "Expected 5 events, got: {}", events.len());

        assert!(
            matches!(
                events[0].payload,
                TokenizedEquityMintEvent::MintRequested { .. }
            ),
            "First event should be MintRequested, got: {:?}",
            events[0].payload
        );

        assert!(
            matches!(
                events[1].payload,
                TokenizedEquityMintEvent::MintAccepted { .. }
            ),
            "Second event should be MintAccepted, got: {:?}",
            events[1].payload
        );

        assert!(
            matches!(
                events[2].payload,
                TokenizedEquityMintEvent::TokensReceived { .. }
            ),
            "Third event should be TokensReceived, got: {:?}",
            events[2].payload
        );

        assert!(
            matches!(
                events[3].payload,
                TokenizedEquityMintEvent::TokensWrapped { .. }
            ),
            "Fourth event should be TokensWrapped, got: {:?}",
            events[3].payload
        );

        let TokenizedEquityMintEvent::RaindexDepositFailed {
            symbol: failed_symbol,
            quantity: failed_quantity,
            ..
        } = &events[4].payload
        else {
            panic!(
                "Fifth event should be RaindexDepositFailed, got: {:?}",
                events[4].payload
            );
        };

        assert_eq!(*failed_symbol, symbol);
        assert_eq!(*failed_quantity, quantity.inner());
    }

    #[tokio::test]
    async fn mint_command_preserves_parameters_in_events() {
        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(
            store.clone(),
            vec![],
            mock_mint_services(),
        ));
        let manager = MintManager::new(cqrs);

        let issuer_request_id = IssuerRequestId::new("params-test");
        let symbol = Symbol::new("TSLA").unwrap();
        let quantity = FractionalShares::new(dec!(42.5));
        let wallet = address!("0xabcdef0123456789abcdef0123456789abcdef01");

        manager
            .execute_mint(&issuer_request_id, symbol.clone(), quantity, wallet)
            .await
            .unwrap();

        let events = store.load_events(&issuer_request_id.0).await.unwrap();

        let TokenizedEquityMintEvent::MintRequested {
            symbol: req_symbol,
            quantity: req_quantity,
            wallet: req_wallet,
            ..
        } = &events[0].payload
        else {
            panic!("Expected MintRequested event");
        };

        assert_eq!(*req_symbol, symbol);
        assert_eq!(*req_quantity, quantity.inner());
        assert_eq!(*req_wallet, wallet);

        let TokenizedEquityMintEvent::Completed {
            symbol: completed_symbol,
            quantity: completed_quantity,
            ..
        } = &events[5].payload
        else {
            panic!("Expected Completed event");
        };

        assert_eq!(*completed_symbol, symbol);
        assert_eq!(*completed_quantity, quantity.inner());
    }

    #[tokio::test]
    async fn different_symbols_can_be_minted_independently() {
        let store = MemStore::default();
        let cqrs = Arc::new(CqrsFramework::new(
            store.clone(),
            vec![],
            mock_mint_services(),
        ));
        let manager = MintManager::new(cqrs);
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let aapl_id = IssuerRequestId::new("aapl-mint");
        let aapl = Symbol::new("AAPL").unwrap();
        manager
            .execute_mint(
                &aapl_id,
                aapl.clone(),
                FractionalShares::new(dec!(10.0)),
                wallet,
            )
            .await
            .unwrap();

        let tsla_id = IssuerRequestId::new("tsla-mint");
        let tsla = Symbol::new("TSLA").unwrap();
        manager
            .execute_mint(
                &tsla_id,
                tsla.clone(),
                FractionalShares::new(dec!(20.0)),
                wallet,
            )
            .await
            .unwrap();

        let aapl_events = store.load_events(&aapl_id.0).await.unwrap();
        let tsla_events = store.load_events(&tsla_id.0).await.unwrap();

        assert_eq!(aapl_events.len(), 6);
        assert_eq!(tsla_events.len(), 6);

        let TokenizedEquityMintEvent::Completed {
            symbol: aapl_completed,
            ..
        } = &aapl_events[5].payload
        else {
            panic!("Expected AAPL Completed event");
        };

        let TokenizedEquityMintEvent::Completed {
            symbol: tsla_completed,
            ..
        } = &tsla_events[5].payload
        else {
            panic!("Expected TSLA Completed event");
        };

        assert_eq!(*aapl_completed, aapl);
        assert_eq!(*tsla_completed, tsla);
    }
}
