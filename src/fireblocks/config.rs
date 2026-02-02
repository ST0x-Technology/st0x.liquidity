use std::path::PathBuf;

use clap::Parser;

/// Environment configuration for Fireblocks signing.
#[derive(Parser, Debug, Clone)]
pub(crate) struct FireblocksEnv {
    /// Fireblocks API key
    #[clap(long = "fireblocks-api-key", env = "FIREBLOCKS_API_KEY")]
    pub(crate) api_key: String,

    /// Path to the RSA private key file for Fireblocks API authentication
    #[clap(long = "fireblocks-secret-path", env = "FIREBLOCKS_SECRET_PATH")]
    pub(crate) secret_path: PathBuf,

    /// Fireblocks vault account ID containing the signing key
    #[clap(
        long = "fireblocks-vault-account-id",
        env = "FIREBLOCKS_VAULT_ACCOUNT_ID"
    )]
    pub(crate) vault_account_id: String,

    /// Fireblocks asset ID for the signing key (e.g. "ETH", "ETH_TEST6", "BASE")
    #[clap(
        long = "fireblocks-asset-id",
        env = "FIREBLOCKS_ASSET_ID",
        default_value = "ETH"
    )]
    pub(crate) asset_id: String,

    /// Use Fireblocks sandbox environment
    #[clap(long = "fireblocks-sandbox", env = "FIREBLOCKS_SANDBOX", default_value = "false", action = clap::ArgAction::Set)]
    pub(crate) sandbox: bool,
}
