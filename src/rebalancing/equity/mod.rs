//! Cross-venue equity transfer.
//!
//! [`CrossVenueEquityTransfer`] implements [`CrossVenueTransfer`] in both
//! directions:
//!
//! - **Hedging -> Market-Making** (mint): requests tokenized equity from
//!   Alpaca and deposits it into a Raindex vault.
//! - **Market-Making -> Hedging** (redemption): withdraws tokenized equity
//!   from a Raindex vault and sends it to Alpaca for redemption.

#[cfg(test)]
pub(crate) mod mock;

use alloy::primitives::{Address, TxHash, U256};
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

use st0x_event_sorcery::{SendError, Store};
use st0x_execution::{FractionalShares, SharesConversionError, Symbol};

use super::transfer::{CrossVenueTransfer, HedgingVenue, MarketMakingVenue};
use crate::equity_redemption::{
    DetectionFailure, EquityRedemption, EquityRedemptionCommand, RedemptionAggregateId,
};
use crate::onchain::raindex::{Raindex, RaindexError};
use crate::tokenization::{
    AlpacaTokenizationError, TokenizationRequestStatus, Tokenizer, TokenizerError,
};
use crate::tokenized_equity_mint::{
    IssuerRequestId, TOKENIZED_EQUITY_DECIMALS, TokenizationRequestId, TokenizedEquityMint,
    TokenizedEquityMintCommand,
};
use crate::wrapper::{Wrapper, WrapperError};

/// A quantity of equity in a specific symbol.
#[derive(Debug)]
pub(crate) struct Equity {
    pub(crate) symbol: Symbol,
    pub(crate) quantity: FractionalShares,
}

/// Services shared by both equity transfer aggregates.
///
/// Both `TokenizedEquityMint` (hedging -> market-making) and
/// `EquityRedemption` (market-making -> hedging) need Raindex for
/// vault operations and Tokenizer for Alpaca API interactions.
#[derive(Clone)]
pub(crate) struct EquityTransferServices {
    pub(crate) raindex: Arc<dyn Raindex>,
    pub(crate) tokenizer: Arc<dyn Tokenizer>,
    pub(crate) wrapper: Arc<dyn Wrapper>,
}

#[derive(Debug, Error)]
pub(crate) enum MintError {
    #[error("Aggregate error: {0}")]
    Aggregate(Box<SendError<TokenizedEquityMint>>),
    #[error("Wrapper error: {0}")]
    Wrapper(#[from] WrapperError),
    #[error("Raindex error: {0}")]
    Raindex(#[from] RaindexError),
    #[error(
        "Entity not found after command: expected {expected_state} \
         for {issuer_request_id}"
    )]
    EntityNotFound {
        issuer_request_id: IssuerRequestId,
        expected_state: &'static str,
    },
}

impl From<SendError<TokenizedEquityMint>> for MintError {
    fn from(error: SendError<TokenizedEquityMint>) -> Self {
        Self::Aggregate(Box::new(error))
    }
}

#[derive(Debug, Error)]
pub(crate) enum RedemptionError {
    #[error(transparent)]
    Send(#[from] SendError<EquityRedemption>),
    #[error(transparent)]
    Raindex(#[from] RaindexError),
    #[error(transparent)]
    Alpaca(#[from] AlpacaTokenizationError),
    #[error(transparent)]
    Tokenizer(#[from] TokenizerError),
    #[error(transparent)]
    SharesConversion(#[from] SharesConversionError),
    #[error("Entity not found after command: {aggregate_id}")]
    EntityNotFound { aggregate_id: RedemptionAggregateId },
    #[error("Token send to Alpaca failed: {entity:?}")]
    SendFailed { entity: EquityRedemption },
    #[error("Unexpected entity: {entity:?}")]
    UnexpectedEntity { entity: EquityRedemption },
    #[error("Unexpected tokenization status: still pending after polling")]
    UnexpectedPendingStatus,
    #[error("Redemption was rejected by Alpaca")]
    Rejected,
}

/// Orchestrates equity transfers between Raindex and Alpaca.
///
/// Holds CQRS stores for both directions and domain service traits for
/// vault operations and tokenization. External code interacts with
/// transfers only through [`CrossVenueTransfer::transfer()`].
pub(crate) struct CrossVenueEquityTransfer {
    raindex: Arc<dyn Raindex>,
    tokenizer: Arc<dyn Tokenizer>,
    wrapper: Arc<dyn Wrapper>,
    wallet: Address,
    mint_store: Arc<Store<TokenizedEquityMint>>,
    redemption_store: Arc<Store<EquityRedemption>>,
}

impl CrossVenueEquityTransfer {
    pub(crate) fn new(
        raindex: Arc<dyn Raindex>,
        tokenizer: Arc<dyn Tokenizer>,
        wrapper: Arc<dyn Wrapper>,
        wallet: Address,
        mint_store: Arc<Store<TokenizedEquityMint>>,
        redemption_store: Arc<Store<EquityRedemption>>,
    ) -> Self {
        Self {
            raindex,
            tokenizer,
            wrapper,
            wallet,
            mint_store,
            redemption_store,
        }
    }

    /// Loads the aggregate after Poll and extracts shares_minted from
    /// the TokensReceived state.
    async fn load_shares_minted(
        &self,
        issuer_request_id: &IssuerRequestId,
    ) -> Result<U256, MintError> {
        let entity = self
            .mint_store
            .load(issuer_request_id)
            .await?
            .ok_or_else(|| MintError::EntityNotFound {
                issuer_request_id: issuer_request_id.clone(),
                expected_state: "TokensReceived",
            })?;

        match entity {
            TokenizedEquityMint::TokensReceived { shares_minted, .. } => Ok(shares_minted),
            other => {
                error!(?other, "Expected TokensReceived after Poll");
                Err(MintError::EntityNotFound {
                    issuer_request_id: issuer_request_id.clone(),
                    expected_state: "TokensReceived",
                })
            }
        }
    }

    /// Sends the Redeem command to withdraw from Raindex vault.
    async fn withdraw_from_raindex(
        &self,
        aggregate_id: &RedemptionAggregateId,
        symbol: &Symbol,
        quantity: FractionalShares,
        token: Address,
        amount: U256,
    ) -> Result<(), RedemptionError> {
        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Redeem {
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    token,
                    amount,
                },
            )
            .await?;

        Ok(())
    }

    /// Unwraps ERC-4626 tokens and sends to Alpaca, returning the
    /// redemption tx hash.
    async fn unwrap_and_send(
        &self,
        aggregate_id: &RedemptionAggregateId,
    ) -> Result<TxHash, RedemptionError> {
        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::UnwrapTokens)
            .await?;

        info!("Tokens unwrapped, sending to Alpaca");

        self.redemption_store
            .send(aggregate_id, EquityRedemptionCommand::SendTokens)
            .await?;

        let entity = self.redemption_store.load(aggregate_id).await?.ok_or(
            RedemptionError::EntityNotFound {
                aggregate_id: aggregate_id.clone(),
            },
        )?;

        match entity {
            EquityRedemption::TokensSent { redemption_tx, .. } => Ok(redemption_tx),
            entity @ EquityRedemption::Failed { .. } => Err(RedemptionError::SendFailed { entity }),
            entity => {
                error!(?entity, "Unexpected entity after SendTokens command");
                Err(RedemptionError::UnexpectedEntity { entity })
            }
        }
    }

    /// Polls for redemption detection and records it.
    async fn poll_detection(
        &self,
        aggregate_id: &RedemptionAggregateId,
        tx_hash: &TxHash,
    ) -> Result<TokenizationRequestId, RedemptionError> {
        let detected = match self.tokenizer.poll_for_redemption(tx_hash).await {
            Ok(req) => req,
            Err(error) => {
                warn!(%error, "Polling for redemption detection failed");
                let failure = match &error {
                    TokenizerError::Alpaca(AlpacaTokenizationError::PollTimeout { .. }) => {
                        DetectionFailure::Timeout
                    }
                    TokenizerError::Alpaca(other) => DetectionFailure::ApiError {
                        status_code: other.status_code().map(|status| status.as_u16()),
                    },
                };

                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::FailDetection { failure },
                    )
                    .await?;

                return Err(error.into());
            }
        };

        self.redemption_store
            .send(
                aggregate_id,
                EquityRedemptionCommand::Detect {
                    tokenization_request_id: detected.id.clone(),
                },
            )
            .await?;

        Ok(detected.id)
    }

    /// Polls for completion and finalizes the redemption.
    async fn poll_completion(
        &self,
        aggregate_id: &RedemptionAggregateId,
        request_id: &TokenizationRequestId,
    ) -> Result<(), RedemptionError> {
        let completed = match self
            .tokenizer
            .poll_redemption_until_complete(request_id)
            .await
        {
            Ok(req) => req,
            Err(error) => {
                warn!(%error, "Polling for completion failed");
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: error.to_string(),
                        },
                    )
                    .await?;
                return Err(error.into());
            }
        };

        match completed.status {
            TokenizationRequestStatus::Completed => {
                self.redemption_store
                    .send(aggregate_id, EquityRedemptionCommand::Complete)
                    .await?;
                Ok(())
            }
            TokenizationRequestStatus::Rejected => {
                self.redemption_store
                    .send(
                        aggregate_id,
                        EquityRedemptionCommand::RejectRedemption {
                            reason: "Alpaca rejected the redemption request".to_string(),
                        },
                    )
                    .await?;
                Err(RedemptionError::Rejected)
            }
            TokenizationRequestStatus::Pending => {
                warn!("poll_redemption_until_complete returned Pending status");
                Err(RedemptionError::UnexpectedPendingStatus)
            }
        }
    }
}

/// Hedging -> Market-Making: tokenize equity on Alpaca and deposit into
/// Raindex vault.
#[async_trait]
impl CrossVenueTransfer<HedgingVenue, MarketMakingVenue> for CrossVenueEquityTransfer {
    type Asset = Equity;
    type Error = MintError;

    #[instrument(skip(self), fields(symbol = %asset.symbol, quantity = ?asset.quantity))]
    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        let Equity { symbol, quantity } = asset;
        let issuer_request_id = IssuerRequestId::new(Uuid::new_v4().to_string());

        debug!(issuer_request_id = %issuer_request_id.0, wallet = %self.wallet, "Requesting mint");

        self.mint_store
            .send(
                &issuer_request_id,
                TokenizedEquityMintCommand::RequestMint {
                    issuer_request_id: issuer_request_id.clone(),
                    symbol: symbol.clone(),
                    quantity: quantity.inner(),
                    wallet: self.wallet,
                },
            )
            .await?;

        info!("Mint request accepted, polling for completion");

        self.mint_store
            .send(&issuer_request_id, TokenizedEquityMintCommand::Poll)
            .await?;

        let shares_minted = self.load_shares_minted(&issuer_request_id).await?;

        info!(%shares_minted, "Tokens received, wrapping into ERC-4626 shares");

        let unwrapped_token = self.wrapper.lookup_unwrapped(&symbol)?;
        let (wrap_tx_hash, wrapped_shares) = self
            .wrapper
            .to_wrapped(unwrapped_token, shares_minted, self.wallet)
            .await?;

        self.mint_store
            .send(
                &issuer_request_id,
                TokenizedEquityMintCommand::WrapTokens {
                    wrap_tx_hash,
                    wrapped_shares,
                },
            )
            .await?;

        info!(%wrap_tx_hash, %wrapped_shares, "Tokens wrapped, depositing to Raindex vault");

        let vault_id = self.raindex.lookup_vault_id(unwrapped_token).await?;
        let vault_deposit_tx_hash = self
            .raindex
            .deposit(
                unwrapped_token,
                vault_id,
                wrapped_shares,
                TOKENIZED_EQUITY_DECIMALS,
            )
            .await?;

        self.mint_store
            .send(
                &issuer_request_id,
                TokenizedEquityMintCommand::DepositToVault {
                    vault_deposit_tx_hash,
                },
            )
            .await?;

        info!(%vault_deposit_tx_hash, "Mint workflow completed");
        Ok(())
    }
}

/// Market-Making -> Hedging: withdraw tokenized equity from Raindex vault
/// and send to Alpaca for redemption.
#[async_trait]
impl CrossVenueTransfer<MarketMakingVenue, HedgingVenue> for CrossVenueEquityTransfer {
    type Asset = Equity;
    type Error = RedemptionError;

    #[instrument(skip(self), fields(symbol = %asset.symbol, quantity = ?asset.quantity))]
    async fn transfer(&self, asset: Self::Asset) -> Result<(), Self::Error> {
        let Equity { symbol, quantity } = asset;

        let (token, _vault_id) = self.raindex.lookup_vault_info(&symbol).await?;
        let amount = quantity.to_u256_18_decimals()?;
        let aggregate_id = RedemptionAggregateId::new(Uuid::new_v4().to_string());

        info!(%token, %amount, aggregate_id = %aggregate_id.0, "Starting equity transfer to hedging venue");

        self.withdraw_from_raindex(&aggregate_id, &symbol, quantity, token, amount)
            .await?;

        info!("Withdrawn from Raindex, unwrapping and sending to Alpaca");

        let redemption_tx = self.unwrap_and_send(&aggregate_id).await?;

        info!(%redemption_tx, "Tokens sent, polling for detection");
        let request_id = self.poll_detection(&aggregate_id, &redemption_tx).await?;

        info!(%request_id, "Redemption detected, awaiting completion");
        self.poll_completion(&aggregate_id, &request_id).await?;

        info!("Equity transfer to hedging venue completed successfully");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::Address;
    use rust_decimal_macros::dec;
    use sqlx::SqlitePool;
    use std::sync::Arc;

    use st0x_event_sorcery::test_store;
    use st0x_execution::{FractionalShares, Symbol};

    use super::*;
    use crate::onchain::mock::MockRaindex;
    use crate::tokenization::mock::{MockCompletionOutcome, MockDetectionOutcome, MockTokenizer};
    use crate::wrapper::mock::MockWrapper;

    fn mock_services() -> EquityTransferServices {
        EquityTransferServices {
            raindex: Arc::new(MockRaindex::new()),
            tokenizer: Arc::new(MockTokenizer::new()),
            wrapper: Arc::new(MockWrapper::new()),
        }
    }

    async fn create_equity_transfer(
        tokenizer: Arc<dyn Tokenizer>,
        raindex: Arc<dyn Raindex>,
    ) -> CrossVenueEquityTransfer {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::migrate!().run(&pool).await.unwrap();
        let services = mock_services();

        let mint_store = Arc::new(test_store(pool.clone(), services.clone()));
        let redemption_store = Arc::new(test_store(pool, services));

        let wrapper: Arc<dyn Wrapper> = Arc::new(MockWrapper::new());

        CrossVenueEquityTransfer::new(
            raindex,
            tokenizer,
            wrapper,
            Address::random(),
            mint_store,
            redemption_store,
        )
    }

    #[tokio::test]
    async fn mint_transfer_sends_mint_and_deposit_commands() {
        let transfer =
            create_equity_transfer(Arc::new(MockTokenizer::new()), Arc::new(MockRaindex::new()))
                .await;

        CrossVenueTransfer::<HedgingVenue, MarketMakingVenue>::transfer(
            &transfer,
            Equity {
                symbol: Symbol::new("AAPL").unwrap(),
                quantity: FractionalShares::new(dec!(100.0)),
            },
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn redemption_transfer_full_workflow_succeeds() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Completed),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer = create_equity_transfer(tokenizer, raindex).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
                &transfer,
                Equity {
                    symbol: Symbol::new("TEST").unwrap(),
                    quantity: FractionalShares::new(dec!(50)),
                },
            ),
        )
        .await
        .expect("redemption transfer timed out")
        .unwrap();
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_timeout() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::Timeout));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer = create_equity_transfer(tokenizer, raindex).await;

        let error = CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &transfer,
            Equity {
                symbol: Symbol::new("TEST").unwrap(),
                quantity: FractionalShares::new(dec!(50)),
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_detection_api_error() {
        let tokenizer: Arc<dyn Tokenizer> =
            Arc::new(MockTokenizer::new().with_detection_outcome(MockDetectionOutcome::ApiError));
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer = create_equity_transfer(tokenizer, raindex).await;

        let error = CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &transfer,
            Equity {
                symbol: Symbol::new("TEST").unwrap(),
                quantity: FractionalShares::new(dec!(50)),
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Tokenizer(_)),
            "Expected Tokenizer error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_completion_rejection() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Rejected),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer = create_equity_transfer(tokenizer, raindex).await;

        let error = CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &transfer,
            Equity {
                symbol: Symbol::new("TEST").unwrap(),
                quantity: FractionalShares::new(dec!(50)),
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, RedemptionError::Rejected),
            "Expected Rejected error, got: {error:?}"
        );
    }

    #[tokio::test]
    async fn redemption_transfer_fails_on_pending_status() {
        let tokenizer: Arc<dyn Tokenizer> = Arc::new(
            MockTokenizer::new()
                .with_detection_outcome(MockDetectionOutcome::Detected)
                .with_completion_outcome(MockCompletionOutcome::Pending),
        );
        let raindex: Arc<dyn Raindex> = Arc::new(MockRaindex::new());

        let transfer = create_equity_transfer(tokenizer, raindex).await;

        let error = CrossVenueTransfer::<MarketMakingVenue, HedgingVenue>::transfer(
            &transfer,
            Equity {
                symbol: Symbol::new("TEST").unwrap(),
                quantity: FractionalShares::new(dec!(50)),
            },
        )
        .await
        .unwrap_err();

        assert!(
            matches!(error, RedemptionError::UnexpectedPendingStatus),
            "Expected UnexpectedPendingStatus error, got: {error:?}"
        );
    }
}
