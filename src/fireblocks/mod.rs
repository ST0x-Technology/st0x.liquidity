mod config;
mod signer;

use alloy::network::EthereumWallet;
use alloy::primitives::{Address, B256};
use alloy::signers::Signer;
use alloy::signers::local::PrivateKeySigner;
use clap::Parser;

pub(crate) use config::FireblocksEnv;
pub(crate) use signer::{FireblocksError, FireblocksSigner};

/// Resolved signer: an `EthereumWallet` and the corresponding address.
///
/// Consumers don't need to know which backend produced the wallet.
pub(crate) struct ResolvedSigner {
    pub(crate) wallet: EthereumWallet,
    pub(crate) address: Address,
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

    /// Fireblocks asset ID for the signing key (e.g. "ETH", "ETH_TEST6", "BASE")
    #[clap(long, env, default_value = "ETH")]
    fireblocks_asset_id: String,

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
    #[error(transparent)]
    Fireblocks(#[from] FireblocksError),
    #[error("invalid EVM private key")]
    InvalidPrivateKey(#[source] alloy::signers::k256::ecdsa::Error),
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
                    asset_id: self.fireblocks_asset_id,
                    sandbox: self.fireblocks_sandbox,
                }))
            }
            (None, Some(key)) => Ok(SignerConfig::Local(key)),
            (None, None) => Err(SignerConfigError::NeitherConfigured),
        }
    }
}

impl SignerConfig {
    /// Resolve the signer config into a wallet + address.
    ///
    /// For Fireblocks, this makes an async API call to fetch the vault address.
    /// For local keys, this is a synchronous derivation.
    pub(crate) async fn resolve(&self) -> Result<ResolvedSigner, SignerResolveError> {
        match self {
            Self::Fireblocks(env) => {
                let signer = FireblocksSigner::new(env).await?;
                let address = signer.address();
                let wallet = EthereumWallet::from(signer);
                Ok(ResolvedSigner { wallet, address })
            }
            Self::Local(key) => {
                let signer = PrivateKeySigner::from_bytes(key)
                    .map_err(SignerResolveError::InvalidPrivateKey)?;
                let address = signer.address();
                let wallet = EthereumWallet::from(signer);
                Ok(ResolvedSigner { wallet, address })
            }
        }
    }

    /// Derive the address without creating a wallet.
    ///
    /// For local keys this is synchronous. For Fireblocks, this requires an
    /// async API call (same as `resolve()`).
    pub(crate) async fn address(&self) -> Result<Address, SignerResolveError> {
        match self {
            Self::Fireblocks(env) => {
                let signer = FireblocksSigner::new(env).await?;
                Ok(signer.address())
            }
            Self::Local(key) => {
                let signer = PrivateKeySigner::from_bytes(key)
                    .map_err(SignerResolveError::InvalidPrivateKey)?;
                Ok(signer.address())
            }
        }
    }
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
            fireblocks_asset_id: "ETH".to_string(),
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
            fireblocks_asset_id: "ETH".to_string(),
            fireblocks_sandbox: false,
        };
        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::NeitherConfigured)),
            "Expected NeitherConfigured error, got {result:?}"
        );
    }

    #[test]
    fn signer_env_with_fireblocks_missing_secret_path_fails() {
        let env = SignerEnv {
            evm_private_key: None,
            fireblocks_api_key: Some("test-key".to_string()),
            fireblocks_secret_path: None,
            fireblocks_vault_account_id: Some("0".to_string()),
            fireblocks_asset_id: "ETH".to_string(),
            fireblocks_sandbox: false,
        };
        let result = env.into_config();
        assert!(
            matches!(result, Err(SignerConfigError::MissingSecretPath)),
            "Expected MissingSecretPath error, got {result:?}"
        );
    }

    #[tokio::test]
    async fn local_signer_resolves_to_correct_address() {
        // Private key 0x01 -> address 0x7E5F4552091A69125d5DfCb7b8C2659029395Bdf
        let mut key = B256::ZERO;
        key.0[31] = 1;
        let config = SignerConfig::Local(key);

        let resolved = config.resolve().await.unwrap();

        assert_eq!(
            resolved.address,
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

        assert_eq!(address, resolved.address);
    }
}
