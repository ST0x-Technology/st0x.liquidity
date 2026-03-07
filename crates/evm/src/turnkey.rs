//! Turnkey secure enclave transaction submission.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). Like
//! [`RawPrivateKeyWallet`](super::local::RawPrivateKeyWallet), it wraps
//! the base provider with a [`WalletFiller`] -- the only difference is
//! the signer: `TurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key).

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

impl TurnkeyOrganizationId {
    pub fn new(value: String) -> Self {
        Self(value)
    }
}

/// Hex-encoded P-256 API private key for Turnkey authentication
/// (secret, lives in encrypted config).
#[derive(Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyApiPrivateKey(String);

impl TurnkeyApiPrivateKey {
    pub fn new(value: String) -> Self {
        Self(value)
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

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
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::U256;
    use alloy::providers::ext::AnvilApi;
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

    // --- Integration tests (real Turnkey API + local Anvil) ---------
    //
    // These tests sign via the real Turnkey API but submit to a local
    // Anvil instance (no testnet ETH needed -- Anvil funds the address
    // via `anvil_setBalance`).
    //
    // Required env vars (set in `.env`, loaded by direnv):
    //   TURNKEY_API_PRIVATE_KEY  -- hex-encoded P-256 API private key
    //   TURNKEY_ORG_ID           -- Turnkey organization ID
    //   TURNKEY_ADDRESS          -- Ethereum address managed by Turnkey
    //
    // See `.env.example` for the template.

    /// Returns `None` when `TURNKEY_*` env vars are absent, allowing
    /// tests to skip gracefully in environments without credentials.
    fn turnkey_env() -> Option<(String, String, Address)> {
        let api_key = std::env::var("TURNKEY_API_PRIVATE_KEY").ok()?;
        let org_id = std::env::var("TURNKEY_ORG_ID").ok()?;
        let address: Address = std::env::var("TURNKEY_ADDRESS")
            .ok()?
            .parse()
            .expect("TURNKEY_ADDRESS must be valid hex address");

        Some((api_key, org_id, address))
    }

    /// Spin up Anvil, fund the Turnkey address, and return a
    /// `TurnkeyWallet` connected to the local node.
    async fn integration_wallet(
        api_key: String,
        org_id: String,
        address: Address,
    ) -> (TurnkeyWallet<impl Provider + Clone>, AnvilInstance) {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        // Fund the Turnkey-managed address on the local Anvil node.
        provider
            .anvil_set_balance(address, U256::from(10) * U256::from(10).pow(U256::from(18)))
            .await
            .expect("anvil_setBalance should succeed");

        let wallet = TurnkeyWallet::new(TurnkeyCtx {
            api_private_key: TurnkeyApiPrivateKey::new(api_key),
            organization_id: TurnkeyOrganizationId::new(org_id),
            address,
            provider,
            required_confirmations: 1,
        })
        .await
        .expect("failed to construct TurnkeyWallet from env vars");

        (wallet, anvil)
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_wallet_address_matches_configured() {
        let (api_key, org_id, expected) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) = integration_wallet(api_key, org_id, expected).await;

        assert_eq!(wallet.address(), expected);
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_signs_and_submits_transaction() {
        let (api_key, org_id, address) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) = integration_wallet(api_key, org_id, address).await;
        let self_address = wallet.address();

        // 0-value self-transfer: minimal gas (21000), exercises the
        // full Turnkey signing round-trip.
        let receipt = wallet
            .send(self_address, Bytes::new(), "integration test self-transfer")
            .await
            .expect("Turnkey signing and submission should succeed");

        assert!(
            receipt.status(),
            "self-transfer should succeed, tx: {}",
            receipt.transaction_hash
        );
        assert_eq!(
            receipt.from, self_address,
            "transaction should be from the configured wallet"
        );
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_concurrent_signing() {
        let (api_key, org_id, address) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) = integration_wallet(api_key, org_id, address).await;
        let self_address = wallet.address();

        // Two parallel 0-value self-transfers to verify nonce
        // management doesn't collide under concurrent signing.
        let (receipt_a, receipt_b) = tokio::join!(
            wallet.send(self_address, Bytes::new(), "concurrent-a"),
            wallet.send(self_address, Bytes::new(), "concurrent-b"),
        );

        let receipt_a = receipt_a.expect("concurrent send A should succeed");
        let receipt_b = receipt_b.expect("concurrent send B should succeed");

        assert!(receipt_a.status(), "tx A should succeed");
        assert!(receipt_b.status(), "tx B should succeed");
        assert_ne!(
            receipt_a.transaction_hash, receipt_b.transaction_hash,
            "transactions must have different hashes"
        );
    }
}
