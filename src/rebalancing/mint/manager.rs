//! MintManager orchestrates the TokenizedEquityMint workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `TokenizedEquityMint` aggregate
//! to execute the full mint lifecycle: request -> poll -> receive tokens -> finalize.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use rust_decimal::Decimal;
use st0x_execution::Symbol;
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::{Mint, MintError};
use crate::alpaca_tokenization::{
    AlpacaTokenizationService, TokenizationRequest, TokenizationRequestStatus,
};
use crate::lifecycle::{Lifecycle, Never};
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::{
    IssuerRequestId, ReceiptId, TokenizedEquityMint, TokenizedEquityMintCommand,
};

pub(crate) struct MintManager<P, ES>
where
    P: Provider + Clone,
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    service: Arc<AlpacaTokenizationService<P>>,
    cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
}

impl<P, ES> MintManager<P, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P>>,
        cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
    ) -> Self {
        Self { service, cqrs }
    }

    /// Executes the full mint workflow.
    ///
    /// # Workflow
    ///
    /// 1. Send `RequestMint` command to aggregate
    /// 2. Call `AlpacaTokenizationService::request_mint()`
    /// 3. Send `AcknowledgeAcceptance` with request IDs
    /// 4. Poll Alpaca until terminal status
    /// 5. Send `ReceiveTokens` when Alpaca reports completion
    /// 6. Send `Finalize` to complete
    ///
    /// On permanent errors, sends `Fail` command to transition aggregate to Failed state.
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
                TokenizedEquityMintCommand::RequestMint {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet,
                },
            )
            .await?;

        let alpaca_request = match self.service.request_mint(symbol, quantity, wallet).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Alpaca mint request failed: {e}");
                self.reject_mint(issuer_request_id, format!("Alpaca API error: {e}"))
                    .await?;
                return Err(MintError::Alpaca(e));
            }
        };

        let alpaca_issuer_request_id = alpaca_request
            .issuer_request_id
            .clone()
            .ok_or(MintError::MissingIssuerRequestId)?;

        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id: alpaca_issuer_request_id,
                    tokenization_request_id: alpaca_request.id.clone(),
                },
            )
            .await?;

        info!(tokenization_request_id = %alpaca_request.id, "Mint request accepted, polling for completion");

        let completed_request = match self
            .service
            .poll_mint_until_complete(&alpaca_request.id)
            .await
        {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling failed: {e}");
                self.fail_acceptance(issuer_request_id, format!("Polling failed: {e}"))
                    .await?;
                return Err(MintError::Alpaca(e));
            }
        };

        self.handle_completed_request(issuer_request_id, completed_request)
            .await
    }

    #[instrument(skip(self))]
    async fn reject_mint(
        &self,
        issuer_request_id: &IssuerRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::RejectMint { reason },
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn fail_acceptance(
        &self,
        issuer_request_id: &IssuerRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::FailAcceptance { reason },
            )
            .await?;
        Ok(())
    }

    #[instrument(skip(self, completed_request), fields(status = ?completed_request.status))]
    async fn handle_completed_request(
        &self,
        issuer_request_id: &IssuerRequestId,
        completed_request: TokenizationRequest,
    ) -> Result<(), MintError> {
        match completed_request.status {
            TokenizationRequestStatus::Completed => {
                let tx_hash = completed_request.tx_hash.ok_or(MintError::MissingTxHash)?;
                let shares_minted = decimal_to_u256_18_decimals(completed_request.quantity)?;

                self.cqrs
                    .execute(
                        &issuer_request_id.0,
                        TokenizedEquityMintCommand::ReceiveTokens {
                            tx_hash,
                            receipt_id: ReceiptId(U256::ZERO),
                            shares_minted,
                        },
                    )
                    .await?;

                self.cqrs
                    .execute(&issuer_request_id.0, TokenizedEquityMintCommand::Finalize)
                    .await?;

                info!("Mint workflow completed successfully");
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.fail_acceptance(
                    issuer_request_id,
                    "Mint request rejected by Alpaca".to_string(),
                )
                .await?;
                Err(MintError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_mint_until_complete should not return Pending status")
            }
        }
    }
}

fn decimal_to_u256_18_decimals(value: FractionalShares) -> Result<U256, MintError> {
    let decimal = value.inner();
    let scale_factor = Decimal::from(10u64.pow(18));
    let scaled = decimal
        .checked_mul(scale_factor)
        .ok_or(MintError::DecimalOverflow(value))?;
    let truncated = scaled.trunc();

    if scaled != truncated {
        return Err(MintError::PrecisionLoss(value));
    }

    Ok(U256::from_str_radix(&truncated.to_string(), 10)?)
}

#[async_trait]
impl<P, ES> Mint for MintManager<P, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
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
    use alloy::primitives::{U256, address};
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil, tokenization_mint_path,
        tokenization_requests_path,
    };

    type TestCqrs = CqrsFramework<
        Lifecycle<TokenizedEquityMint, Never>,
        MemStore<Lifecycle<TokenizedEquityMint, Never>>,
    >;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    fn sample_pending_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "pending",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    fn sample_completed_response(id: &str) -> serde_json::Value {
        json!({
            "tokenization_request_id": id,
            "type": "mint",
            "status": "completed",
            "underlying_symbol": "AAPL",
            "token_symbol": "tAAPL",
            "qty": "100.0",
            "issuer": "st0x",
            "network": "base",
            "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
            "issuer_request_id": "issuer_123",
            "tx_hash": "0xabcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
            "created_at": "2024-01-15T10:30:00Z"
        })
    }

    #[test]
    fn decimal_to_u256_converts_fractional() {
        let value = FractionalShares::new(dec!(100.5));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        let expected = U256::from(100_500_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn decimal_to_u256_converts_whole_number() {
        let value = FractionalShares::new(dec!(42));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        let expected = U256::from(42_000_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn decimal_to_u256_converts_zero() {
        let value = FractionalShares::new(dec!(0));
        let result = decimal_to_u256_18_decimals(value).unwrap();

        assert_eq!(result, U256::ZERO);
    }

    #[tokio::test]
    async fn execute_mint_happy_path() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_123"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("mint_123")]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-001"), symbol, quantity, wallet)
            .await;

        assert!(result.is_ok(), "execute_mint failed: {result:?}");

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn execute_mint_rejected_by_alpaca() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_456"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "mint_456",
                    "type": "mint",
                    "status": "rejected",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "100.0",
                    "issuer": "st0x",
                    "network": "base",
                    "wallet_address": "0x1234567890abcdef1234567890abcdef12345678",
                    "issuer_request_id": "issuer_123",
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-002"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn execute_mint_api_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(500).body("Internal Server Error");
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint_impl(&IssuerRequestId::new("mint-003"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Alpaca(_))));

        mint_mock.assert();
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_mint_impl() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path(tokenization_mint_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("trait_test"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("trait_test")]));
        });

        let mint_trait: &dyn Mint = &manager;

        let result = mint_trait
            .execute_mint(
                &IssuerRequestId::new("trait-001"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
            )
            .await;

        assert!(result.is_ok());

        mint_mock.assert();
        poll_mock.assert();
    }
}
