//! RedemptionManager orchestrates the EquityRedemption workflow.
//!
//! Coordinates between `AlpacaTokenizationService` and the `EquityRedemption` aggregate
//! to execute the full redemption lifecycle: send tokens -> poll detection -> poll completion.

use alloy::primitives::{Address, U256};
use alloy::providers::Provider;
use alloy::signers::Signer;
use cqrs_es::{AggregateError, CqrsFramework, EventStore};
use st0x_broker::Symbol;
use std::sync::Arc;
use thiserror::Error;
use tracing::{info, instrument, warn};

use crate::alpaca_tokenization::{
    AlpacaTokenizationError, AlpacaTokenizationService, TokenizationRequestStatus,
};
use crate::equity_redemption::{EquityRedemption, EquityRedemptionCommand, EquityRedemptionError};
use crate::lifecycle::{Lifecycle, Never};
use crate::shares::FractionalShares;

#[derive(Debug, Error)]
pub(crate) enum RedemptionError {
    #[error("Alpaca API error: {0}")]
    Alpaca(#[from] AlpacaTokenizationError),

    #[error("Aggregate error: {0}")]
    Aggregate(#[from] AggregateError<EquityRedemptionError>),

    #[error("Redemption was rejected by Alpaca")]
    Rejected,
}

pub(crate) struct RedemptionManager<P, S, ES>
where
    P: Provider + Clone,
    S: Signer + Clone + Sync,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    service: Arc<AlpacaTokenizationService<P, S>>,
    cqrs: Arc<CqrsFramework<Lifecycle<EquityRedemption, Never>, ES>>,
}

impl<P, S, ES> RedemptionManager<P, S, ES>
where
    P: Provider + Clone + Send + Sync + 'static,
    S: Signer + Clone + Send + Sync + 'static,
    ES: EventStore<Lifecycle<EquityRedemption, Never>>,
{
    pub(crate) fn new(
        service: Arc<AlpacaTokenizationService<P, S>>,
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
    pub(crate) async fn execute_redemption(
        &self,
        aggregate_id: &str,
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
                aggregate_id,
                EquityRedemptionCommand::SendTokens {
                    symbol,
                    quantity: quantity.0,
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
                        aggregate_id,
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
                aggregate_id,
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
                        aggregate_id,
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
                    .execute(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;

                info!("Redemption workflow completed successfully");
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.cqrs
                    .execute(
                        aggregate_id,
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

#[cfg(test)]
mod tests {
    use alloy::primitives::{U256, address};
    use cqrs_es::CqrsFramework;
    use cqrs_es::mem_store::MemStore;
    use httpmock::prelude::*;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::alpaca_tokenization::tests::{
        TEST_REDEMPTION_WALLET, create_test_service_from_mock, setup_anvil,
    };

    type TestCqrs = CqrsFramework<
        Lifecycle<EquityRedemption, Never>,
        MemStore<Lifecycle<EquityRedemption, Never>>,
    >;

    fn create_test_cqrs() -> Arc<TestCqrs> {
        let store = MemStore::default();
        Arc::new(CqrsFramework::new(store, vec![], ()))
    }

    #[tokio::test]
    async fn test_execute_redemption_send_failure() {
        let server = MockServer::start();
        let (_anvil, endpoint, key) = setup_anvil();
        let service = Arc::new(
            create_test_service_from_mock(&server, &endpoint, &key, TEST_REDEMPTION_WALLET).await,
        );
        let cqrs = create_test_cqrs();
        let manager = RedemptionManager::new(service, cqrs);

        let symbol = Symbol::new("AAPL").unwrap();
        let quantity = FractionalShares(dec!(100.0));
        let token = address!("0x1234567890abcdef1234567890abcdef12345678");
        let amount = U256::from(100_000_000_000_000_000_000_u128);

        let result = manager
            .execute_redemption("redemption-001", symbol, quantity, token, amount)
            .await;

        assert!(matches!(result, Err(RedemptionError::Alpaca(_))));
    }
}
