//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `EquityRedemption` aggregate
//! to execute the full redemption lifecycle: send tokens -> poll detection -> poll completion.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use st0x_execution::{FractionalShares, Symbol};
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::{Redeem, RedemptionError};
use crate::alpaca_tokenization::{AlpacaTokenizationService, TokenizationRequestStatus};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::lifecycle::{Lifecycle, Never};

pub(crate) struct RedemptionManager<P, ES>
where
    P: Provider + Clone,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    service: Arc<AlpacaTokenizationService<P>>,
    cqrs: Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>,
}

impl<P, ES> RedemptionManager<P, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P>>,
        cqrs: Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>,
    ) -> Self {
        Self { service, cqrs }
    }

    /// Executes the full redemption workflow.
    ///
    /// # Workflow
    ///
    /// 1. Send tokens to Alpaca redemption wallet
    /// 2. Send `SendTokens` command to aggregate
    /// 3. Poll Alpaca until redemption is detected
    /// 4. Send `Detect` with tokenization_request_id
    /// 5. Poll Alpaca until terminal status
    /// 6. Send `Complete` when Alpaca reports completion
    ///
    /// On errors, sends appropriate failure commands (`FailDetection`, `RejectRedemption`).
    #[instrument(skip(self), fields(%symbol, ?quantity, %token, %amount))]
    async fn execute_redemption_impl(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        info!(%symbol, ?quantity, %token, %amount, "Starting redemption workflow");

        let tx_hash = match self.service.send_for_redemption(token, amount).await {
            Ok(hash) => hash,
            Err(e) => {
                warn!("Failed to send tokens for redemption: {e}");
                return Err(RedemptionError::Alpaca(e));
            }
        };

        self.cqrs
            .execute(
                &aggregate_id.0,
                EquityRedemptionCommand::SendTokens {
                    symbol,
                    quantity: quantity.inner(),
                    redemption_wallet: self.service.redemption_wallet(),
                    tx_hash,
                },
            )
            .await?;

        info!(%tx_hash, "Tokens sent, polling for detection");

        let detected = match self.service.poll_for_redemption(&tx_hash).await {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for redemption detection failed: {e}");
                self.cqrs
                    .execute(
                        &aggregate_id.0,
                        EquityRedemptionCommand::FailDetection {
                            reason: format!("Detection polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        self.cqrs
            .execute(
                &aggregate_id.0,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        info!(
            tokenization_request_id = %detected.id,
            "Redemption detected, polling for completion"
        );

        let completed = match self
            .service
            .poll_redemption_until_complete(&detected.id)
            .await
        {
            Ok(req) => req,
            Err(e) => {
                warn!("Polling for completion failed: {e}");
                self.cqrs
                    .execute(
                        &aggregate_id.0,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: format!("Completion polling failed: {e}"),
                        },
                    )
                    .await?;
                return Err(RedemptionError::Alpaca(e));
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.cqrs
                    .execute(&aggregate_id.0, EquityRedemptionCommand::Complete)
                    .await?;

                info!("Redemption workflow completed successfully");
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.cqrs
                    .execute(
                        &aggregate_id.0,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Redemption rejected by Alpaca".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                unreachable!("poll_redemption_until_complete should not return Pending status")
            }
        }
    }
}

#[async_trait]
impl<P, ES> Redeem for RedemptionManager<P, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<EquityRedemption, Never>> + Send + Sync,
    ES::AC: Send,
{
    async fn execute_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.execute_redemption_impl(aggregate_id, symbol, quantity, token, amount)
            .await
    }
}

#[cfg(test)]
mod tests {
    use alloy::network::EthereumWallet;
    use alloy::primitives::address;
    use alloy::providers::ProviderBuilder;
    use alloy::providers::ext::AnvilApi as _;
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;
    use serde_json::json;

    use super::*;
    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil,
        tokenization_requests_path,
    };
    use crate::bindings::{IERC20, TestERC20};

    type TestCqrs = CqrsFramework<
        Lifecycle<EquityRedemption, Never>,
        MemStore<Lifecycle<EquityRedemption, Never>>,
    >;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    #[tokio::test]
    async fn execute_redemption_send_failure() {
        let server = httpmock::MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs);

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares::new(dec!(100.0));
        let token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let amount = U256::from(100_000_000_000_000_000_000_u128);

        let result = manager
            .execute_redemption_impl(
                &RedemptionAggregateId::new("redemption-001"),
                symbol,
                quantity,
                token,
                amount,
            )
            .await;

        assert!(matches!(result, Err(RedemptionError::Alpaca(_))));
    }

    #[tokio::test]
    async fn trait_impl_delegates_to_execute_redemption_impl() {
        let server = httpmock::MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs);

        let redeem_trait: &dyn Redeem = &manager;

        let result = redeem_trait
            .execute_redemption(
                &RedemptionAggregateId::new("trait-test"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(50.0)),
                address!("0x1234567890abcdef1234567890abcdef12345678"),
                U256::from(50_000_000_000_000_000_000_u128),
            )
            .await;

        // Without mocked token contract, this will fail at send_for_redemption
        assert!(matches!(result, Err(RedemptionError::Alpaca(_))));
    }

    /// Discovers the deterministic tx_hash that Anvil will produce for a given
    /// ERC20 transfer by executing the transfer, capturing the hash, then
    /// reverting to the pre-transfer snapshot.
    async fn discover_deterministic_tx_hash(
        provider: &impl Provider,
        token_address: Address,
        recipient: Address,
        amount: U256,
    ) -> alloy::primitives::TxHash {
        let snapshot_id = provider.anvil_snapshot().await.unwrap();

        let erc20 = IERC20::new(token_address, provider);
        let receipt = erc20
            .transfer(recipient, amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();
        let tx_hash = receipt.transaction_hash;

        provider.anvil_revert(snapshot_id).await.unwrap();

        tx_hash
    }

    #[tokio::test]
    async fn execute_redemption_happy_path() {
        let server = httpmock::MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();

        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer.clone());
        let provider = ProviderBuilder::new()
            .wallet(wallet)
            .connect(&endpoint)
            .await
            .unwrap();

        // Deploy TestERC20 and mint tokens for the redemption transfer
        let token = TestERC20::deploy(&provider).await.unwrap();
        let token_address = *token.address();
        let transfer_amount = U256::from(30_000_000_000_000_000_000_u128); // 30 * 10^18

        token
            .mint(signer.address(), transfer_amount)
            .send()
            .await
            .unwrap()
            .get_receipt()
            .await
            .unwrap();

        // Anvil is deterministic: same sender + nonce + calldata = same tx_hash.
        // Execute the transfer once to capture the hash, then revert so the
        // real manager can execute the same transfer and get the same hash.
        let expected_tx_hash = discover_deterministic_tx_hash(
            &provider,
            token_address,
            TEST_REDEMPTION_WALLET,
            transfer_amount,
        )
        .await;

        let detection_mock = server.mock(|when, then| {
            when.method(GET)
                .path(tokenization_requests_path())
                .query_param("type", "redeem");
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_happy_path",
                    "type": "redeem",
                    "status": "pending",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "30.0",
                    "issuer": "st0x",
                    "network": "base",
                    "tx_hash": expected_tx_hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let completion_mock = server.mock(|when, then| {
            when.method(GET).path(tokenization_requests_path());
            then.status(200)
                .header("content-type", "application/json")
                .json_body(json!([{
                    "tokenization_request_id": "redeem_happy_path",
                    "type": "redeem",
                    "status": "completed",
                    "underlying_symbol": "AAPL",
                    "token_symbol": "tAAPL",
                    "qty": "30.0",
                    "issuer": "st0x",
                    "network": "base",
                    "tx_hash": expected_tx_hash,
                    "created_at": "2024-01-15T10:30:00Z"
                }]));
        });

        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs);

        manager
            .execute_redemption_impl(
                &RedemptionAggregateId::new("redemption-happy"),
                Symbol::new("AAPL").unwrap(),
                FractionalShares::new(dec!(30)),
                token_address,
                transfer_amount,
            )
            .await
            .unwrap();

        // Verify onchain balances after the ERC20 transfer
        let erc20_check = IERC20::new(token_address, &provider);
        let sender_balance = erc20_check
            .balanceOf(signer.address())
            .call()
            .await
            .unwrap();
        assert_eq!(sender_balance, U256::ZERO);
        let recipient_balance = erc20_check
            .balanceOf(TEST_REDEMPTION_WALLET)
            .call()
            .await
            .unwrap();
        assert_eq!(recipient_balance, transfer_amount);

        detection_mock.assert();
        completion_mock.assert();
    }
}
