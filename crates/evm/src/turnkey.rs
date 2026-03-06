//! Turnkey secure enclave transaction submission.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). Like
//! [`RawPrivateKeyWallet`](super::local::RawPrivateKeyWallet), it wraps
//! the base provider with a [`WalletFiller`] -- the only difference is
//! the signer: `TurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key). This module is only compiled when
//! the `turnkey` feature is enabled.

use alloy::network::{Ethereum, EthereumWallet};
use alloy::primitives::{Address, Bytes};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder};
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy_signer_turnkey::{TurnkeySigner, TurnkeySignerError};
use async_trait::async_trait;
use serde::Deserialize;
use tracing::info;

use crate::{Evm, EvmError, Wallet};

/// Turnkey organization identifier (non-secret, lives in plaintext
/// config).
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyOrganizationId(String);

/// Hex-encoded P-256 API private key for Turnkey authentication
/// (secret, lives in encrypted config).
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyApiPrivateKey(String);

impl std::fmt::Debug for TurnkeyApiPrivateKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

/// Errors specific to the Turnkey signing backend.
#[derive(Debug, thiserror::Error)]
pub enum TurnkeyError {
    #[error("Turnkey signer error: {0}")]
    Signer(#[from] TurnkeySignerError),
}

/// Construction context for `TurnkeyWallet`.
///
/// Contains everything needed to build a wallet: Turnkey API
/// credentials, the wallet address, and a base provider. Callers
/// construct this directly via its public fields and pass it to
/// [`TurnkeyWallet::new`].
pub struct TurnkeyCtx<P> {
    pub api_private_key: TurnkeyApiPrivateKey,
    pub organization_id: TurnkeyOrganizationId,
    pub address: Address,
    pub provider: P,
    pub required_confirmations: u64,
}

impl<P> std::fmt::Debug for TurnkeyCtx<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyCtx")
            .field("api_private_key", &self.api_private_key)
            .field("organization_id", &self.organization_id)
            .field("address", &self.address)
            .field("required_confirmations", &self.required_confirmations)
            .finish_non_exhaustive()
    }
}

/// Provider type produced by wrapping a base provider with default
/// fillers and a [`WalletFiller`] backed by a Turnkey signer.
type SignerProvider<P> = FillProvider<
    JoinFill<
        JoinFill<
            Identity,
            JoinFill<GasFiller, JoinFill<BlobGasFiller, JoinFill<NonceFiller, ChainIdFiller>>>,
        >,
        WalletFiller<EthereumWallet>,
    >,
    P,
    Ethereum,
>;

/// Wallet that signs transactions via Turnkey's secure enclaves.
///
/// Stores both the base provider (exposed via [`Evm::provider()`] for
/// read-only chain access) and a signing provider (used internally by
/// [`Wallet::send()`]). This separation ensures `type Provider = P`,
/// matching the existing `dyn Wallet<Provider = RootProvider>` trait
/// objects used throughout the codebase.
pub struct TurnkeyWallet<P: Provider> {
    /// Base provider for read-only chain access (view calls, balance
    /// checks, block subscriptions).
    provider: P,
    /// Provider wrapped with gas/nonce/chain-id/wallet fillers for
    /// transaction signing and submission.
    signing_provider: SignerProvider<P>,
    address: Address,
    required_confirmations: u64,
}

impl<P: Provider> std::fmt::Debug for TurnkeyWallet<P> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyWallet")
            .field("address", &self.address)
            .field("required_confirmations", &self.required_confirmations)
            .finish_non_exhaustive()
    }
}

impl<P: Provider + Clone + Send + Sync + 'static> TurnkeyWallet<P> {
    /// Creates a new `TurnkeyWallet` from a context containing API
    /// credentials, wallet address, and base provider.
    ///
    /// Constructs a `TurnkeySigner` using the P-256 API private key,
    /// wraps it in an `EthereumWallet`, and builds the signing
    /// provider with standard fillers. The base provider is cloned
    /// and stored separately for read-only access.
    pub async fn new(ctx: TurnkeyCtx<P>) -> Result<Self, EvmError> {
        let chain_id = ctx.provider.get_chain_id().await?;

        let TurnkeyApiPrivateKey(api_key_hex) = &ctx.api_private_key;
        let TurnkeyOrganizationId(organization_id) = ctx.organization_id;

        let signer =
            TurnkeySigner::from_api_key(api_key_hex, organization_id, ctx.address, Some(chain_id))?;

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = ctx.provider.clone();

        let signing_provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect_provider(ctx.provider);

        Ok(Self {
            provider: base_provider,
            signing_provider,
            address: ctx.address,
            required_confirmations: ctx.required_confirmations,
        })
    }

    /// Creates a `TurnkeyWallet` from a pre-built `TurnkeyClient`.
    ///
    /// Used in tests to inject a client configured with a mock server
    /// base URL. Production code should use [`new`](Self::new).
    #[cfg(test)]
    async fn from_client(
        client: alloy_signer_turnkey::TurnkeyClient,
        organization_id: String,
        address: Address,
        provider: P,
        required_confirmations: u64,
    ) -> Result<Self, EvmError> {
        let chain_id = provider.get_chain_id().await?;

        let signer = TurnkeySigner::new(client, organization_id, address, Some(chain_id));

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = provider.clone();

        let signing_provider = ProviderBuilder::new()
            .wallet(eth_wallet)
            .connect_provider(provider);

        Ok(Self {
            provider: base_provider,
            signing_provider,
            address,
            required_confirmations,
        })
    }
}

#[async_trait]
impl<P> Evm for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Provider = P;

    fn provider(&self) -> &P {
        &self.provider
    }
}

#[async_trait]
impl<P> Wallet for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    fn address(&self) -> Address {
        self.address
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        info!(%contract, note, "Submitting Turnkey contract call");

        let tx = TransactionRequest::default()
            .to(contract)
            .input(calldata.into());

        let pending = self.signing_provider.send_transaction(tx).await?;

        info!(tx_hash = %pending.tx_hash(), note, "Transaction submitted");

        let receipt = pending
            .with_required_confirmations(self.required_confirmations)
            .get_receipt()
            .await?;

        info!(tx_hash = %receipt.transaction_hash, note, "Transaction confirmed");

        Ok(receipt)
    }
}

#[cfg(test)]
mod tests {
    use alloy::node_bindings::Anvil;
    use alloy_signer_turnkey::{TurnkeyClient, TurnkeyClientBuilder, TurnkeyP256ApiKey};
    use httpmock::MockServer;

    use super::*;

    /// Generate a fresh P-256 API key for testing.
    fn test_api_key() -> TurnkeyP256ApiKey {
        TurnkeyP256ApiKey::generate()
    }

    /// Build a `TurnkeyClient` that sends requests to the mock server.
    fn mock_client(server: &MockServer) -> TurnkeyClient {
        let api_key = test_api_key();

        TurnkeyClientBuilder::new()
            .api_key(api_key)
            .base_url(server.base_url())
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn send_signing_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_raw_payload");
            then.status(500)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "error": "internal server error"
                }));
        });

        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let client = mock_client(&server);

        let wallet = TurnkeyWallet::from_client(
            client,
            "org-test".to_string(),
            Address::random(),
            provider,
            1,
        )
        .await
        .unwrap();

        let error = wallet
            .send(Address::ZERO, Bytes::new(), "test signing failure")
            .await
            .unwrap_err();

        // Turnkey signer errors surface through alloy's transport
        // layer as Transport(LocalUsageError(Signer(Other(...)))) --
        // the signer failure happens inside `send_transaction`.
        assert!(
            matches!(error, EvmError::Transport(_)),
            "expected Transport error wrapping signer failure, got: {error:?}"
        );
        let error_str = format!("{error:?}");
        assert!(
            error_str.contains("TurnkeyClient"),
            "expected TurnkeyClient error in chain, got: {error_str}"
        );
    }

    #[tokio::test]
    async fn new_constructs_wallet_with_correct_address() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());
        let server = MockServer::start();
        let client = mock_client(&server);
        let expected_address = Address::random();

        let wallet = TurnkeyWallet::from_client(
            client,
            "org-test".to_string(),
            expected_address,
            provider,
            1,
        )
        .await
        .unwrap();

        assert_eq!(wallet.address(), expected_address);
    }
}
