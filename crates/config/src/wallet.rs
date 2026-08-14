//! Top-level wallet infrastructure, independent of rebalancing.
//!
//! Provides signing wallets for Base and Ethereum chains. Used by CLI
//! commands that need to sign on-chain transactions (vault ops, CCTP
//! bridging, token wrapping, Alpaca deposits/withdrawals) regardless
//! of whether the system is in Standalone or Rebalancing trading mode.

use alloy::providers::RootProvider;
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use serde::Deserialize;
use std::sync::Arc;
use tracing::info;
use url::Url;

use st0x_evm::{Wallet, WalletCtx as EvmWalletCtx, WalletKind};

const REQUIRED_CONFIRMATIONS: u64 = 3;
const RPC_MAX_RETRIES: u32 = 10;
const RPC_INITIAL_BACKOFF_MS: u64 = 1000;
const RPC_COMPUTE_UNITS_PER_SECOND: u64 = 100;

/// Extracts just the `kind` discriminant from the wallet TOML table,
/// ignoring backend-specific fields that vary by wallet type.
#[derive(Deserialize)]
struct WalletKindTag {
    kind: WalletKind,
}

/// Newtype over [`toml::Value`] implementing [`Parser`] so
/// [`WalletKind::try_into_wallet`] can deserialize any
/// `DeserializeOwned` type from raw TOML config/secrets.
struct TomlValue(toml::Value);

impl st0x_evm::Parser for TomlValue {
    type Error = st0x_evm::EvmError;

    fn parse<Target: serde::de::DeserializeOwned>(self) -> Result<Target, Self::Error> {
        Target::deserialize(self.0)
            .map_err(|error| st0x_evm::EvmError::WalletConfigParse(Box::new(error)))
    }
}

/// Error type for wallet construction.
#[derive(Debug, thiserror::Error)]
pub enum WalletCtxError {
    #[error("invalid wallet config: {0}")]
    WalletConfig(#[from] toml::de::Error),
    #[error("failed to build the wallet RPC HTTP client: {0}")]
    HttpClient(#[from] reqwest::Error),
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// Pre-built signing wallets for Base and Ethereum chains.
///
/// Independent of rebalancing — any trading mode can optionally
/// configure a wallet for manual CLI operations.
#[derive(Clone)]
pub struct OnchainWalletCtx {
    base: Arc<dyn Wallet<Provider = RootProvider>>,
    ethereum: Arc<dyn Wallet<Provider = RootProvider>>,
    hyperevm: Arc<dyn Wallet<Provider = RootProvider>>,
}

impl OnchainWalletCtx {
    /// Build wallets for all chains from raw TOML config/secrets and RPC URLs.
    ///
    /// Without wallet features, `WalletKind` is uninhabited so
    /// deserialization always fails at the `?` — making later clones
    /// appear redundant to clippy.
    #[cfg_attr(
        not(any(feature = "wallet-turnkey", feature = "wallet-private-key")),
        allow(clippy::redundant_clone)
    )]
    pub async fn new(
        wallet_config: toml::Value,
        wallet_secrets: toml::Value,
        base_rpc_url: Url,
        ethereum_rpc_url: Url,
        hyperevm_rpc_url: Url,
    ) -> Result<Self, WalletCtxError> {
        let WalletKindTag { kind } = WalletKindTag::deserialize(wallet_config.clone())?;

        let (base_wallet, ethereum_wallet, hyperevm_wallet) = tokio::try_join!(
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                base_rpc_url,
            ),
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                ethereum_rpc_url,
            ),
            build_wallet(&kind, wallet_config, wallet_secrets, hyperevm_rpc_url,),
        )?;

        info!(
            target: "wallet",
            wallet = %base_wallet.address(),
            "Initialized onchain wallet (Base + Ethereum + HyperEVM)"
        );

        Ok(Self {
            base: base_wallet,
            ethereum: ethereum_wallet,
            hyperevm: hyperevm_wallet,
        })
    }

    // The accessors below are deliberately trivial getters: the fields are
    // private across the crate boundary, and wallet selection logic stays
    // out of this crate on purpose. The consumer pairs a wallet with its
    // Alpaca wire value in a single match (see the CLI's
    // tokenization_network_context) so the two cannot diverge.
    pub fn base_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.base
    }

    pub fn ethereum_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.ethereum
    }

    pub fn hyperevm_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.hyperevm
    }
}

/// Creates an HTTP RPC client with a retry layer for transient errors.
///
/// Redirects are disabled: the RPC URL is validated at config load (https or
/// loopback), and following a redirect would let the server route signing
/// traffic to a destination that never passed that validation.
fn http_client_with_retry(url: Url) -> Result<RpcClient, reqwest::Error> {
    let retry_layer = RetryBackoffLayer::new(
        RPC_MAX_RETRIES,
        RPC_INITIAL_BACKOFF_MS,
        RPC_COMPUTE_UNITS_PER_SECOND,
    );
    let http_client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()?;
    let transport = alloy::transports::http::Http::with_client(http_client, url);
    Ok(RpcClient::builder()
        .layer(retry_layer)
        .transport(transport, false))
}

pub async fn build_wallet(
    kind: &WalletKind,
    wallet_config: toml::Value,
    wallet_secrets: toml::Value,
    rpc_url: Url,
) -> Result<Arc<dyn Wallet<Provider = RootProvider>>, WalletCtxError> {
    let provider = RootProvider::new(http_client_with_retry(rpc_url)?);

    Ok(kind
        .try_into_wallet(EvmWalletCtx {
            settings: TomlValue(wallet_config),
            credentials: TomlValue(wallet_secrets),
            provider,
            required_confirmations: REQUIRED_CONFIRMATIONS,
        })
        .await?)
}

#[cfg(any(test, feature = "test-support"))]
impl OnchainWalletCtx {
    /// Create a stub wallet context for tests.
    pub fn stub() -> Self {
        use alloy::primitives::Address;

        let stub_wallet = st0x_evm::StubWallet::stub(Address::ZERO);

        Self {
            base: stub_wallet.clone(),
            ethereum: stub_wallet.clone(),
            hyperevm: stub_wallet,
        }
    }
}

#[cfg(feature = "test-support")]
impl OnchainWalletCtx {
    /// Create from pre-built wallet instances (for e2e tests).
    pub fn from_wallets(
        base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
        ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
        hyperevm_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    ) -> Self {
        Self {
            base: base_wallet,
            ethereum: ethereum_wallet,
            hyperevm: hyperevm_wallet,
        }
    }
}
