//! RedemptionService implements Redeemer by composing RaindexService and AlpacaTokenizationService.

use alloy::primitives::{Address, TxHash, U256};
use alloy::providers::Provider;
use async_trait::async_trait;
use std::sync::Arc;
use tracing::{info, warn};

use crate::equity_redemption::{RedeemError, Redeemer};
use crate::onchain::raindex::{Raindex, RaindexService};
use crate::tokenization::{AlpacaTokenizationService, Tokenizer};

/// Our tokenized equity tokens use 18 decimals.
pub(super) const TOKENIZED_EQUITY_DECIMALS: u8 = 18;

/// Service that implements Redeemer by composing Raindex and Alpaca services.
pub(crate) struct RedemptionService<P>
where
    P: Provider + Clone,
{
    raindex: Arc<RaindexService<P>>,
    alpaca: Arc<AlpacaTokenizationService<P>>,
}

impl<P> RedemptionService<P>
where
    P: Provider + Clone,
{
    pub(crate) fn new(
        raindex: Arc<RaindexService<P>>,
        alpaca: Arc<AlpacaTokenizationService<P>>,
    ) -> Self {
        Self { raindex, alpaca }
    }

    /// Returns a reference to the Alpaca service for polling operations.
    pub(crate) fn alpaca(&self) -> &AlpacaTokenizationService<P> {
        &self.alpaca
    }
}

#[async_trait]
impl<P> Redeemer for RedemptionService<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    async fn withdraw_from_vault(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<TxHash, RedeemError> {
        let vault_id = match self.raindex.lookup_vault_id(token).await {
            Ok(id) => id,
            Err(error) => {
                warn!(%error, %token, "Vault lookup failed");
                return Err(RedeemError::VaultNotFound(token));
            }
        };

        info!(?vault_id, %token, %amount, "Withdrawing tokens from vault");

        match self
            .raindex
            .withdraw(token, vault_id, amount, TOKENIZED_EQUITY_DECIMALS)
            .await
        {
            Ok(tx) => Ok(tx),
            Err(error) => {
                warn!(%error, %token, %amount, "Vault withdrawal failed");
                Err(RedeemError::VaultWithdraw)
            }
        }
    }

    async fn send_for_redemption(
        &self,
        token: Address,
        amount: U256,
    ) -> Result<(Address, TxHash), RedeemError> {
        info!(%token, %amount, "Sending tokens for redemption");

        let tx_hash =
            match Tokenizer::send_for_redemption(self.alpaca.as_ref(), token, amount).await {
                Ok(tx) => tx,
                Err(error) => {
                    warn!(%error, %token, %amount, "Send for redemption failed");
                    return Err(RedeemError::SendForRedemption);
                }
            };

        Ok((Tokenizer::redemption_wallet(self.alpaca.as_ref()), tx_hash))
    }
}
