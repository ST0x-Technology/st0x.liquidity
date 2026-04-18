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
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// Pre-built signing wallets for Base and Ethereum chains.
///
/// Independent of rebalancing — any trading mode can optionally
/// configure a wallet for manual CLI operations.
#[derive(Clone)]
pub struct OnchainWalletCtx {
    base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
}

impl OnchainWalletCtx {
    /// Build wallets for both chains from raw TOML config/secrets and RPC URLs.
    ///
    /// Without wallet features, `WalletKind` is uninhabited so
    /// deserialization always fails at the `?` — making later clones
    /// appear redundant to clippy.
    #[cfg_attr(
        not(any(feature = "wallet-turnkey", feature = "wallet-private-key")),
        allow(clippy::redundant_clone)
    )]
    pub(crate) async fn new(
        wallet_config: toml::Value,
        wallet_secrets: toml::Value,
        base_rpc_url: Url,
        ethereum_rpc_url: Url,
    ) -> Result<Self, WalletCtxError> {
        let WalletKindTag { kind } = WalletKindTag::deserialize(wallet_config.clone())?;

        let (base_wallet, ethereum_wallet) = tokio::try_join!(
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                base_rpc_url,
            ),
            build_wallet(&kind, wallet_config, wallet_secrets, ethereum_rpc_url,),
        )?;

        info!(
            wallet = %base_wallet.address(),
            "Initialized wallet"
        );

        Ok(Self {
            base_wallet,
            ethereum_wallet,
        })
    }

    pub(crate) fn base_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.base_wallet
    }

    pub(crate) fn ethereum_wallet(&self) -> &Arc<dyn Wallet<Provider = RootProvider>> {
        &self.ethereum_wallet
    }
}

/// Creates an HTTP RPC client with retry layer for transient errors.
fn http_client_with_retry(url: Url) -> RpcClient {
    let retry_layer = RetryBackoffLayer::new(
        RPC_MAX_RETRIES,
        RPC_INITIAL_BACKOFF_MS,
        RPC_COMPUTE_UNITS_PER_SECOND,
    );
    RpcClient::builder().layer(retry_layer).http(url)
}

pub(crate) async fn build_wallet(
    kind: &WalletKind,
    wallet_config: toml::Value,
    wallet_secrets: toml::Value,
    rpc_url: Url,
) -> Result<Arc<dyn Wallet<Provider = RootProvider>>, WalletCtxError> {
    let provider = RootProvider::new(http_client_with_retry(rpc_url));

    Ok(kind
        .try_into_wallet(EvmWalletCtx {
            settings: TomlValue(wallet_config),
            credentials: TomlValue(wallet_secrets),
            provider,
            required_confirmations: REQUIRED_CONFIRMATIONS,
        })
        .await?)
}

#[cfg(test)]
impl OnchainWalletCtx {
    /// Create a stub wallet context for tests.
    pub(crate) fn stub() -> Self {
        use alloy::primitives::Address;

        let stub_wallet = crate::test_utils::StubWallet::stub(Address::ZERO);

        Self {
            base_wallet: stub_wallet.clone(),
            ethereum_wallet: stub_wallet,
        }
    }
}

#[cfg(feature = "test-support")]
impl OnchainWalletCtx {
    /// Create from pre-built wallet instances (for e2e tests).
    pub fn from_wallets(
        base_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
        ethereum_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    ) -> Self {
        Self {
            base_wallet,
            ethereum_wallet,
        }
    }
}
