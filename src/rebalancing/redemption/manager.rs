//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `EquityRedemption` aggregate
//! to execute the full redemption lifecycle: unwrap -> send tokens -> poll detection -> poll completion.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use cqrs_es::{CqrsFramework, EventStore};
use st0x_execution::Symbol;
use std::sync::Arc;
use tracing::{info, instrument, warn};

use super::{Redeem, RedemptionError};
use crate::alpaca_tokenization::{
    AlpacaTokenizationService, TokenizationRequest, TokenizationRequestStatus,
};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId};
use crate::lifecycle::{Lifecycle, Never};
use crate::shares::FractionalShares;
use crate::tokenized_equity_mint::TokenizationRequestId;
use crate::vault::VaultService;

pub(crate) struct RedemptionManager<P, ES>
where
    P: Provider + Clone,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    service: Arc<AlpacaTokenizationService<P>>,
    cqrs: Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>,
    vault_service: Arc<VaultService<P>>,
}

impl<P, ES> RedemptionManager<P, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P>>,
        cqrs: Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>,
        vault_service: Arc<VaultService<P>>,
    ) -> Self {
        Self {
            service,
            cqrs,
            vault_service,
        }
    }

    /// Executes the full redemption workflow.
    ///
    /// # Workflow
    ///
    /// 1. If symbol uses wrapped tokens: unwrap via ERC-4626 vault, send `UnwrapTokens`
    /// 2. Send unwrapped tokens to Alpaca redemption wallet
    /// 3. Send `SendTokens` command to aggregate
    /// 4. Poll Alpaca until redemption is detected
    /// 5. Send `Detect` with tokenization_request_id
    /// 6. Poll Alpaca until terminal status
    /// 7. Send `Complete` when Alpaca reports completion
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

        let (token_to_send, amount_to_send) = self
            .maybe_unwrap_tokens(aggregate_id, &symbol, quantity, token, amount)
            .await?;

        let tx_hash = self
            .send_tokens_for_redemption(
                aggregate_id,
                symbol,
                quantity,
                token_to_send,
                amount_to_send,
            )
            .await?;

        let detected = self.poll_for_detection(aggregate_id, &tx_hash).await?;

        self.poll_for_completion(aggregate_id, &detected.id).await
    }

    /// Unwraps tokens if the given token is a wrapped token, otherwise returns the original values.
    async fn maybe_unwrap_tokens(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(Address, U256), RedemptionError> {
        let Some(config) = self.vault_service.get_config_by_wrapped(&token) else {
            return Ok((token, amount));
        };

        let wrapped_token = config.wrapped_token;
        let unwrapped_token = config.unwrapped_token;

        info!(
            %symbol,
            %wrapped_token,
            %unwrapped_token,
            wrapped_amount = %amount,
            "Unwrapping tokens from ERC-4626 vault"
        );

        let owner = self.service.redemption_wallet();
        let (unwrap_tx_hash, unwrapped_amount) = self
            .vault_service
            .unwrap(wrapped_token, amount, owner, owner)
            .await?;

        info!(%unwrap_tx_hash, %unwrapped_amount, "Tokens unwrapped successfully");

        self.cqrs
            .execute(
                &aggregate_id.0,
                EquityRedemptionCommand::UnwrapTokens {
                    symbol: symbol.clone(),
                    quantity: quantity.0,
                    wrapped_amount: amount,
                    unwrap_tx_hash,
                    unwrapped_amount,
                },
            )
            .await?;

        Ok((unwrapped_token, unwrapped_amount))
    }

    /// Sends tokens to the redemption wallet and records the SendTokens command.
    async fn send_tokens_for_redemption(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<alloy::primitives::TxHash, RedemptionError> {
        let tx_hash = self
            .service
            .send_for_redemption(token, amount)
            .await
            .map_err(|e| {
                warn!("Failed to send tokens for redemption: {e}");
                RedemptionError::Alpaca(e)
            })?;

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
        Ok(tx_hash)
    }

    /// Polls Alpaca for redemption detection and records the Detect command.
    async fn poll_for_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &alloy::primitives::TxHash,
    ) -> Result<TokenizationRequest, RedemptionError> {
        let detected = match self.service.poll_for_redemption(tx_hash).await {
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

        Ok(detected)
    }

    /// Polls Alpaca for completion and handles the final status.
    async fn poll_for_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self
            .service
            .poll_redemption_until_complete(request_id)
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
    use alloy::providers::Provider;
    use alloy::signers::local::PrivateKeySigner;
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::alpaca_tokenization::AlpacaTokenizationService;
    use crate::alpaca_tokenization::tests::{TEST_ACCOUNT_ID, TEST_REDEMPTION_WALLET, setup_anvil};
    use crate::vault::WrappedTokenRegistry;

    type TestCqrs = CqrsFramework<
        Lifecycle<EquityRedemption, Never>,
        MemStore<Lifecycle<EquityRedemption, Never>>,
    >;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    /// Creates a test setup with matching provider types for service and vault_service.
    fn create_test_setup<P: Provider + Clone>(
        server: &httpmock::MockServer,
        provider: P,
    ) -> (Arc<AlpacaTokenizationService<P>>, Arc<VaultService<P>>) {
        let service = Arc::new(AlpacaTokenizationService::new(
            server.base_url(),
            TEST_ACCOUNT_ID,
            "test_api_key".to_string(),
            "test_api_secret".to_string(),
            provider.clone(),
            TEST_REDEMPTION_WALLET,
        ));

        let vault_service = Arc::new(VaultService::new(provider, WrappedTokenRegistry::empty()));

        (service, vault_service)
    }

    async fn create_test_provider() -> impl Provider + Clone {
        let (anvil, endpoint, key) = setup_anvil();
        let signer = PrivateKeySigner::from_bytes(&key).unwrap();
        let wallet = EthereumWallet::from(signer);

        let provider = alloy::providers::ProviderBuilder::new()
            .wallet(wallet)
            .connect(&endpoint)
            .await
            .unwrap();

        // Keep anvil alive by leaking it (it's dropped when the test ends anyway)
        std::mem::forget(anvil);

        provider
    }

    #[tokio::test]
    async fn execute_redemption_send_failure() {
        let server = httpmock::MockServer::start();
        let provider = create_test_provider().await;
        let (service, vault_service) = create_test_setup(&server, provider);
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs, vault_service);

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
        let provider = create_test_provider().await;
        let (service, vault_service) = create_test_setup(&server, provider);
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs, vault_service);

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
}
