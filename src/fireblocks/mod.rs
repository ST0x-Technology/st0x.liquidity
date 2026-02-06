mod config;
mod contract_call;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::providers::Provider;
use alloy::providers::ProviderBuilder;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;
use fireblocks_sdk::ClientBuilder;
use std::sync::Arc;

pub(crate) use config::{ChainAssetIds, FireblocksEnv};
pub(crate) use contract_call::{
    AlloyContractCaller, ContractCallError, ContractCallSubmitter, FireblocksContractCallError,
    FireblocksContractCaller,
};

/// Resolved signer with the address and backend-specific data.
///
/// For local signing, holds an `EthereumWallet`. For Fireblocks, holds the
/// environment config needed to create submitters per-chain.
#[derive(Debug)]
pub(crate) enum ResolvedSigner {
    Local {
        address: Address,
        wallet: EthereumWallet,
    },
    Fireblocks {
        address: Address,
        env: FireblocksEnv,
    },
}

impl ResolvedSigner {
    pub(crate) fn address(&self) -> Address {
        match self {
            Self::Local { address, .. } | Self::Fireblocks { address, .. } => *address,
        }
    }

    /// Returns the wallet for local signing, or `None` for Fireblocks.
    pub(crate) fn wallet(&self) -> Option<&EthereumWallet> {
        match self {
            Self::Local { wallet, .. } => Some(wallet),
            Self::Fireblocks { .. } => None,
        }
    }

    /// Creates a `ContractCallSubmitter` for the given read provider.
    ///
    /// For local signing, wraps the provider with the wallet so that
    /// alloy's filler chain handles gas, nonce, and signing. For Fireblocks,
    /// creates a `FireblocksContractCaller` that submits CONTRACT_CALL
    /// transactions and fetches receipts from the read provider.
    pub(crate) fn create_submitter<P: Provider + Clone + Send + Sync + 'static>(
        &self,
        read_provider: P,
        required_confirmations: u64,
    ) -> Result<Arc<dyn ContractCallSubmitter>, FireblocksContractCallError> {
        match self {
            Self::Local { wallet, .. } => {
                let wallet_provider = ProviderBuilder::new()
                    .wallet(wallet.clone())
                    .connect_provider(read_provider);
                Ok(Arc::new(AlloyContractCaller::new(
                    wallet_provider,
                    required_confirmations,
                )))
            }
            Self::Fireblocks { env, .. } => {
                Ok(Arc::new(FireblocksContractCaller::new(env, read_provider)?))
            }
        }
    }
}

/// Determines which signing backend to use at runtime.
///
/// If `FIREBLOCKS_API_KEY` is set, Fireblocks is used. Otherwise falls back to
/// `EVM_PRIVATE_KEY` for local signing. Both are optional in clap; validation
/// ensures exactly one is configured.
#[derive(Parser, Debug, Clone)]
pub(crate) struct SignerEnv {
    /// Private key for signing EVM transactions (local signer, mutually exclusive with Fireblocks)
    #[clap(long, env)]
    evm_private_key: Option<B256>,

    /// Fireblocks API key
    #[clap(long, env)]
    fireblocks_api_key: Option<String>,

    /// Path to the RSA private key file for Fireblocks API authentication
    #[clap(long, env)]
    fireblocks_secret_path: Option<std::path::PathBuf>,

    /// Fireblocks vault account ID containing the signing key
    #[clap(long, env)]
    fireblocks_vault_account_id: Option<String>,

    /// Mapping of chain ID to Fireblocks asset ID, e.g. "1:ETH,8453:BASECHAIN_ETH"
    #[clap(long, env, default_value = "1:ETH", value_parser = config::parse_chain_asset_ids)]
    fireblocks_chain_asset_ids: ChainAssetIds,

    /// Use Fireblocks sandbox environment
    #[clap(long, env, default_value = "false", action = clap::ArgAction::Set)]
    fireblocks_sandbox: bool,
}

/// Parsed signer configuration.
#[derive(Debug, Clone)]
pub(crate) enum SignerConfig {
    Fireblocks(FireblocksEnv),
    Local(B256),
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SignerConfigError {
    #[error("exactly one of FIREBLOCKS_API_KEY or EVM_PRIVATE_KEY must be set")]
    NeitherConfigured,
    #[error("both FIREBLOCKS_API_KEY and EVM_PRIVATE_KEY are set; use only one")]
    BothConfigured,
    #[error("FIREBLOCKS_SECRET_PATH is required when FIREBLOCKS_API_KEY is set")]
    MissingSecretPath,
    #[error("FIREBLOCKS_VAULT_ACCOUNT_ID is required when FIREBLOCKS_API_KEY is set")]
    MissingVaultAccountId,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SignerResolveError {
    #[error("invalid EVM private key")]
    InvalidPrivateKey(#[source] alloy::signers::k256::ecdsa::Error),
    #[error(transparent)]
    FireblocksContractCall(#[from] FireblocksContractCallError),
}

impl SignerEnv {
    pub(crate) fn into_config(self) -> Result<SignerConfig, SignerConfigError> {
        match (self.fireblocks_api_key, self.evm_private_key) {
            (Some(_), Some(_)) => Err(SignerConfigError::BothConfigured),
            (Some(api_key), None) => {
                let secret_path = self
                    .fireblocks_secret_path
                    .ok_or(SignerConfigError::MissingSecretPath)?;
                let vault_account_id = self
                    .fireblocks_vault_account_id
                    .ok_or(SignerConfigError::MissingVaultAccountId)?;

                Ok(SignerConfig::Fireblocks(FireblocksEnv {
                    api_key,
                    secret_path,
                    vault_account_id,
                    chain_asset_ids: self.fireblocks_chain_asset_ids,
                    sandbox: self.fireblocks_sandbox,
                }))
            }
            (None, Some(key)) => Ok(SignerConfig::Local(key)),
            (None, None) => Err(SignerConfigError::NeitherConfigured),
        }
    }
}

impl SignerConfig {
    /// Resolve the signer config into a `ResolvedSigner`.
    ///
    /// For Fireblocks, makes an async API call to fetch the vault address.
    /// For local keys, synchronously derives the address.
    pub(crate) async fn resolve(&self) -> Result<ResolvedSigner, SignerResolveError> {
        match self {
            Self::Fireblocks(env) => {
                let address = fetch_fireblocks_address(env).await?;
                Ok(ResolvedSigner::Fireblocks {
                    address,
                    env: env.clone(),
                })
            }
            Self::Local(key) => {
                let signer = PrivateKeySigner::from_bytes(key)
                    .map_err(SignerResolveError::InvalidPrivateKey)?;
                let address = signer.address();
                let wallet = EthereumWallet::from(signer);
                Ok(ResolvedSigner::Local { wallet, address })
            }
        }
    }

    /// Derive the address without creating a full `ResolvedSigner`.
    ///
    /// For local keys this is synchronous. For Fireblocks, this requires an
    /// async API call to fetch the vault's deposit address.
    pub(crate) async fn address(&self) -> Result<Address, SignerResolveError> {
        match self {
            Self::Fireblocks(env) => Ok(fetch_fireblocks_address(env).await?),
            Self::Local(key) => {
                let signer = PrivateKeySigner::from_bytes(key)
                    .map_err(SignerResolveError::InvalidPrivateKey)?;
                Ok(signer.address())
            }
        }
    }
}

/// Fetches the vault deposit address from Fireblocks using the SDK client.
///
/// Reads the RSA secret key from disk, builds the client, and queries the
/// vault's deposit addresses. Reused by both `resolve()` and `address()`.
async fn fetch_fireblocks_address(
    env: &FireblocksEnv,
) -> Result<Address, FireblocksContractCallError> {
    let secret =
        std::fs::read(&env.secret_path).map_err(|e| FireblocksContractCallError::ReadSecret {
            path: env.secret_path.clone(),
            source: e,
        })?;

    let mut builder = ClientBuilder::new(&env.api_key, &secret);
    if env.sandbox {
        builder = builder.use_sandbox();
    }
    let client = builder
        .build()
        .map_err(|e| FireblocksContractCallError::ClientBuild(Box::new(e)))?;

    let default_asset_id = env.chain_asset_ids.default_asset_id();

    let addresses = client
        .addresses(&env.vault_account_id, default_asset_id)
        .await
        .map_err(|e| FireblocksContractCallError::FetchAddresses(Box::new(e)))?;

    let address_str = addresses
        .first()
        .and_then(|a| a.address.as_deref())
        .ok_or_else(|| FireblocksContractCallError::NoAddress {
            vault_id: env.vault_account_id.clone(),
            asset_id: default_asset_id.to_string(),
        })?;

    address_str
        .parse::<Address>()
        .map_err(|e| FireblocksContractCallError::InvalidAddress {
            address: address_str.to_string(),
            source: e,
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signer_env_with_local_key_produces_local_config() {
        let key = B256::from([1u8; 32]);

        let env = SignerEnv {
            evm_private_key: Some(key),
            fireblocks_api_key: None,
            fireblocks_secret_path: None,
            fireblocks_vault_account_id: None,
            fireblocks_chain_asset_ids: config::parse_chain_asset_ids("1:ETH").unwrap(),
            fireblocks_sandbox: false,
        };
        let config = env.into_config().unwrap();
        assert!(
            matches!(config, SignerConfig::Local(k) if k == key),
            "Expected Local config, got {config:?}"
        );
    }

    #[test]
    fn signer_env_with_neither_fails() {
        let env = SignerEnv {
            evm_private_key: None,
            fireblocks_api_key: None,
            fireblocks_secret_path: None,
            fireblocks_vault_account_id: None,
            fireblocks_chain_asset_ids: config::parse_chain_asset_ids("1:ETH").unwrap(),
            fireblocks_sandbox: false,
        };
        assert!(matches!(
            env.into_config().unwrap_err(),
            SignerConfigError::NeitherConfigured
        ));
    }

    #[test]
    fn signer_env_with_both_configured_fails() {
        let env = SignerEnv {
            evm_private_key: Some(B256::from([1u8; 32])),
            fireblocks_api_key: Some("test-key".to_string()),
            fireblocks_secret_path: Some("/tmp/key.pem".into()),
            fireblocks_vault_account_id: Some("0".to_string()),
            fireblocks_chain_asset_ids: config::parse_chain_asset_ids("1:ETH").unwrap(),
            fireblocks_sandbox: false,
        };
        assert!(matches!(
            env.into_config().unwrap_err(),
            SignerConfigError::BothConfigured
        ));
    }

    #[test]
    fn signer_env_with_fireblocks_missing_secret_path_fails() {
        let env = SignerEnv {
            evm_private_key: None,
            fireblocks_api_key: Some("test-key".to_string()),
            fireblocks_secret_path: None,
            fireblocks_vault_account_id: Some("0".to_string()),
            fireblocks_chain_asset_ids: config::parse_chain_asset_ids("1:ETH").unwrap(),
            fireblocks_sandbox: false,
        };
        assert!(matches!(
            env.into_config().unwrap_err(),
            SignerConfigError::MissingSecretPath
        ));
    }

    #[test]
    fn signer_env_with_fireblocks_missing_vault_account_id_fails() {
        let env = SignerEnv {
            evm_private_key: None,
            fireblocks_api_key: Some("test-key".to_string()),
            fireblocks_secret_path: Some("/tmp/key.pem".into()),
            fireblocks_vault_account_id: None,
            fireblocks_chain_asset_ids: config::parse_chain_asset_ids("1:ETH").unwrap(),
            fireblocks_sandbox: false,
        };
        assert!(matches!(
            env.into_config().unwrap_err(),
            SignerConfigError::MissingVaultAccountId
        ));
    }

    #[tokio::test]
    async fn local_signer_resolves_to_correct_address() {
        // Private key 0x01 -> address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
        let mut key = B256::ZERO;
        key.0[31] = 1;
        let config = SignerConfig::Local(key);

        let resolved = config.resolve().await.unwrap();

        assert_eq!(
            resolved.address(),
            "0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf"
                .parse::<Address>()
                .unwrap()
        );
    }

    #[tokio::test]
    async fn local_signer_address_matches_resolve() {
        let mut key = B256::ZERO;
        key.0[31] = 1;
        let config = SignerConfig::Local(key);

        let address = config.address().await.unwrap();
        let resolved = config.resolve().await.unwrap();

        assert_eq!(address, resolved.address());
    }
}
