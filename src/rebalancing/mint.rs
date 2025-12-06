//! MintManager orchestrates the TokenizedEquityMint workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `TokenizedEquityMint` aggregate
//! to execute the full mint lifecycle: request -> poll -> receive tokens -> finalize.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use rust_decimal::Decimal;
use st0x_broker::Symbol;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, warn};

use crate::alpaca_tokenization::{
    AlpacaTokenizationError, AlpacaTokenizationService, TokenizationRequest,
    TokenizationRequestStatus,
};
use crate::lifecycle::{Lifecycle, Never};
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::{
    IssuerRequestId, ReceiptId, TokenizedEquityMint, TokenizedEquityMintCommand,
    TokenizedEquityMintError,
};

#[derive(Debug, Error)]
pub(crate) enum MintError {
    #[error("Alpaca API error: {0}")]
    Alpaca(#[from] AlpacaTokenizationError),

    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<TokenizedEquityMintError>),

    #[error("Mint request was rejected by Alpaca")]
    Rejected,

    #[error("Missing issuer_request_id in Alpaca response")]
    MissingIssuerRequestId,

    #[error("Missing tx_hash in completed Alpaca response")]
    MissingTxHash,
}

pub(crate) struct MintManager<P, S, ES>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    service: Arc<AlpacaTokenizationService<P, S>>,
    cqrs: Arc<CqrsFramework<Lifecycle<TokenizedEquityMint, Never>, ES>>,
}

impl<P, S, ES> MintManager<P, S, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    S: Signer + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<TokenizedEquityMint, Never>>,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P, S>>,
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
    pub(crate) async fn execute_mint(
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
                    quantity: quantity.0,
                    wallet,
                },
            )
            .await?;

        let alpaca_request = match self.service.request_mint(symbol, quantity, wallet).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Alpaca mint request failed: {e}");
                self.fail(issuer_request_id, format!("Alpaca API error: {e}"))
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
                self.fail(issuer_request_id, format!("Polling failed: {e}"))
                    .await?;
                return Err(MintError::Alpaca(e));
            }
        };

        self.handle_completed_request(issuer_request_id, completed_request)
            .await
    }

    async fn fail(
        &self,
        issuer_request_id: &IssuerRequestId,
        reason: String,
    ) -> Result<(), MintError> {
        self.cqrs
            .execute(
                &issuer_request_id.0,
                TokenizedEquityMintCommand::Fail { reason },
            )
            .await?;
        Ok(())
    }

    async fn handle_completed_request(
        &self,
        issuer_request_id: &IssuerRequestId,
        completed_request: TokenizationRequest,
    ) -> Result<(), MintError> {
        match completed_request.status {
            TokenizationRequestStatus::Completed => {
                let tx_hash = completed_request.tx_hash.ok_or(MintError::MissingTxHash)?;
                let shares_minted = decimal_to_u256_18_decimals(completed_request.quantity.0);

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
                self.fail(
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

fn decimal_to_u256_18_decimals(value: Decimal) -> U256 {
    let scaled = value * Decimal::from(10u64.pow(18));
    let as_str = scaled.trunc().to_string();
    U256::from_str_radix(&as_str, 10).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy::primitives::address;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil,
    };

    type TestCqrs = CqrsFramework<
        Lifecycle<TokenizedEquityMint, Never>,
        MemStore<Lifecycle<TokenizedEquityMint, Never>>,
    >;

    #[test]
    fn test_decimal_to_u256_18_decimals() {
        let value = dec!(100.5);
        let result = decimal_to_u256_18_decimals(value);

        let expected = U256::from(100_500_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decimal_to_u256_whole_number() {
        let value = dec!(42);
        let result = decimal_to_u256_18_decimals(value);

        let expected = U256::from(42_000_000_000_000_000_000_u128);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_decimal_to_u256_zero() {
        let value = dec!(0);
        let result = decimal_to_u256_18_decimals(value);

        assert_eq!(result, U256::ZERO);
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

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    #[tokio::test]
    async fn test_execute_mint_happy_path() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_123"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([sample_completed_response("mint_123")]));
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&IssuerRequestId::new("mint-001"), symbol, quantity, wallet)
            .await;

        assert!(result.is_ok(), "execute_mint failed: {result:?}");

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_mint_rejected() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(sample_pending_response("mint_456"));
        });

        let poll_mock = server.mock(|when, then| {
            when.method(GET).path("/v2/tokenization/requests");
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
        let quantity = FractionalShares(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&IssuerRequestId::new("mint-002"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_mint_api_error() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = MintManager::new(service, cqrs);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(500).body("Internal Server Error");
        });

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares(dec!(100.0));
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&IssuerRequestId::new("mint-003"), symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Alpaca(_))));

        mint_mock.assert();
    }
}
