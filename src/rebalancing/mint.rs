//! MintManager orchestrates the TokenizedEquityMint workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `TokenizedEquityMint` aggregate
//! to execute the full mint lifecycle: request -> poll -> receive tokens -> finalize.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use cqrs_es::Aggregate;
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
    ReceiptId, TokenizationRequestId, TokenizedEquityMint, TokenizedEquityMintCommand,
    TokenizedEquityMintError,
};

/// Errors that can occur during mint orchestration.
#[derive(Debug, Error)]
pub(crate) enum MintError {
    #[error("Alpaca API error: {0}")]
    Alpaca(#[from] AlpacaTokenizationError),

    #[error("Aggregate error: {0}")]
    Aggregate(#[from] TokenizedEquityMintError),

    #[error("Mint request was rejected by Alpaca")]
    Rejected,

    #[error("Missing issuer_request_id in Alpaca response")]
    MissingIssuerRequestId,

    #[error("Missing tx_hash in completed Alpaca response")]
    MissingTxHash,
}

/// Orchestrates the TokenizedEquityMint workflow.
///
/// Stateless manager that coordinates between the Alpaca tokenization service
/// and the TokenizedEquityMint aggregate. All persistent state lives in the
/// aggregate via the event store.
pub(crate) struct MintManager<P, S>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
{
    service: Arc<AlpacaTokenizationService<P, S>>,
}

impl<P, S> MintManager<P, S>
where
    P: Provider + Clone + Send + Sync + 'static,
    S: Signer + Clone + Send + Sync + 'static,
{
    /// Creates a new MintManager with the given tokenization service.
    pub(crate) fn new(service: Arc<AlpacaTokenizationService<P, S>>) -> Self {
        Self { service }
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
        aggregate: &mut Lifecycle<TokenizedEquityMint, Never>,
        symbol: Symbol,
        quantity: Decimal,
        wallet: Address,
    ) -> Result<(), MintError> {
        // Step 1: Request mint in aggregate
        info!(%symbol, %quantity, %wallet, "Starting mint workflow");

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::RequestMint {
                    symbol: symbol.clone(),
                    quantity,
                    wallet,
                },
                &(),
            )
            .await?;

        for event in events {
            aggregate.apply(event);
        }

        // Step 2: Call Alpaca API
        let fractional_quantity = FractionalShares(quantity);
        let alpaca_result = self
            .service
            .request_mint(symbol.clone(), fractional_quantity, wallet)
            .await;

        let alpaca_request = match alpaca_result {
            Ok(req) => req,
            Err(e) => {
                warn!("Alpaca mint request failed: {e}");
                self.fail_aggregate(aggregate, format!("Alpaca API error: {e}"))
                    .await?;
                return Err(MintError::Alpaca(e));
            }
        };

        // Step 3: Acknowledge acceptance
        let issuer_request_id = alpaca_request
            .issuer_request_id
            .clone()
            .ok_or(MintError::MissingIssuerRequestId)?;

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::AcknowledgeAcceptance {
                    issuer_request_id,
                    tokenization_request_id: alpaca_request.id.clone(),
                },
                &(),
            )
            .await?;

        for event in events {
            aggregate.apply(event);
        }

        info!(
            tokenization_request_id = %alpaca_request.id,
            "Mint request accepted, polling for completion"
        );

        // Step 4: Poll until terminal
        let completed_request = self
            .poll_until_complete(&alpaca_request.id, aggregate)
            .await?;

        // Step 5: Handle terminal status
        match completed_request.status {
            TokenizationRequestStatus::Completed => {
                let tx_hash = completed_request.tx_hash.ok_or(MintError::MissingTxHash)?;

                self.receive_tokens_and_finalize(aggregate, tx_hash, &completed_request)
                    .await?;

                info!("Mint workflow completed successfully");
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.fail_aggregate(aggregate, "Mint request rejected by Alpaca".to_string())
                    .await?;
                Err(MintError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_until_complete should not return Pending status")
            }
        }
    }

    async fn poll_until_complete(
        &self,
        id: &TokenizationRequestId,
        aggregate: &mut Lifecycle<TokenizedEquityMint, Never>,
    ) -> Result<TokenizationRequest, MintError> {
        match self.service.poll_mint_until_complete(id).await {
            Ok(req) => Ok(req),
            Err(e) => {
                warn!("Polling failed: {e}");
                self.fail_aggregate(aggregate, format!("Polling failed: {e}"))
                    .await?;
                Err(MintError::Alpaca(e))
            }
        }
    }

    async fn receive_tokens_and_finalize(
        &self,
        aggregate: &mut Lifecycle<TokenizedEquityMint, Never>,
        tx_hash: TxHash,
        request: &TokenizationRequest,
    ) -> Result<(), MintError> {
        // Convert quantity to U256 for onchain representation (18 decimals)
        let shares_minted = decimal_to_u256_18_decimals(request.quantity.0);

        let events = aggregate
            .handle(
                TokenizedEquityMintCommand::ReceiveTokens {
                    tx_hash,
                    receipt_id: ReceiptId(U256::ZERO), // Alpaca doesn't provide receipt ID
                    shares_minted,
                },
                &(),
            )
            .await?;

        for event in events {
            aggregate.apply(event);
        }

        // Finalize
        let events = aggregate
            .handle(TokenizedEquityMintCommand::Finalize, &())
            .await?;

        for event in events {
            aggregate.apply(event);
        }

        Ok(())
    }

    async fn fail_aggregate(
        &self,
        aggregate: &mut Lifecycle<TokenizedEquityMint, Never>,
        reason: String,
    ) -> Result<(), TokenizedEquityMintError> {
        let events = aggregate
            .handle(TokenizedEquityMintCommand::Fail { reason }, &())
            .await?;

        for event in events {
            aggregate.apply(event);
        }

        Ok(())
    }
}

/// Converts a Decimal to U256 with 18 decimal places.
fn decimal_to_u256_18_decimals(value: Decimal) -> U256 {
    let scaled = value * Decimal::from(10u64.pow(18));
    let as_str = scaled.trunc().to_string();
    U256::from_str_radix(&as_str, 10).unwrap_or(U256::ZERO)
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil,
    };

    #[test]
    fn test_decimal_to_u256_18_decimals() {
        let value = dec!(100.5);
        let result = decimal_to_u256_18_decimals(value);

        // 100.5 * 10^18 = 100_500_000_000_000_000_000
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

    #[tokio::test]
    async fn test_execute_mint_happy_path() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let manager = MintManager::new(service);

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

        let mut aggregate: Lifecycle<TokenizedEquityMint, Never> = Lifecycle::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = dec!(100.0);
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&mut aggregate, symbol, quantity, wallet)
            .await;

        assert!(result.is_ok(), "execute_mint failed: {result:?}");

        let state = aggregate.live().unwrap();
        assert!(
            matches!(state, TokenizedEquityMint::Completed { .. }),
            "expected Completed state, got: {state:?}"
        );

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
        let manager = MintManager::new(service);

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

        let mut aggregate: Lifecycle<TokenizedEquityMint, Never> = Lifecycle::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = dec!(100.0);
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&mut aggregate, symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Rejected)));

        let state = aggregate.live().unwrap();
        assert!(
            matches!(state, TokenizedEquityMint::Failed { .. }),
            "expected Failed state, got: {state:?}"
        );

        mint_mock.assert();
        poll_mock.assert();
    }

    #[tokio::test]
    async fn test_execute_mint_api_error_fails_aggregate() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let manager = MintManager::new(service);

        let mint_mock = server.mock(|when, then| {
            when.method(POST).path("/v2/tokenization/mint");
            then.status(500).body("Internal Server Error");
        });

        let mut aggregate: Lifecycle<TokenizedEquityMint, Never> = Lifecycle::default();
        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = dec!(100.0);
        let wallet = address!("0x1234567890abcdef1234567890abcdef12345678");

        let result = manager
            .execute_mint(&mut aggregate, symbol, quantity, wallet)
            .await;

        assert!(matches!(result, Err(MintError::Alpaca(_))));

        let state = aggregate.live().unwrap();
        assert!(
            matches!(state, TokenizedEquityMint::Failed { .. }),
            "expected Failed state, got: {state:?}"
        );

        mint_mock.assert();
    }
}
