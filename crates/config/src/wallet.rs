//! Top-level wallet infrastructure, independent of rebalancing.
//!
//! Provides one signing wallet per chain. Used by CLI
//! commands that need to sign on-chain transactions (vault ops, CCTP
//! bridging, token wrapping, Alpaca deposits/withdrawals) regardless
//! of whether the system is in Standalone or Rebalancing trading mode.

use alloy::providers::RootProvider;
use alloy::rpc::client::RpcClient;
use alloy::transports::layers::RetryBackoffLayer;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::info;
use url::Url;

use st0x_evm::{Chain, Wallet, WalletCtx as EvmWalletCtx, WalletKind};
use st0x_rpc::JwtEnv;

use crate::chain_rpc::{ChainRpcError, ChainRpcTimeouts, chain_rpc_client};
use crate::loader::StartupNotice;

const RPC_MAX_RETRIES: u32 = 10;
const RPC_INITIAL_BACKOFF_MS: u64 = 1000;
const RPC_COMPUTE_UNITS_PER_SECOND: u64 = 100;
const RPC_REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

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
    #[error("failed to build the {chain} wallet RPC client")]
    ChainRpc {
        chain: Chain,
        #[source]
        source: ChainRpcError,
    },
    #[error(
        "[chains.{chain}] rpc_url must use https; http is allowed only for loopback \
         hosts (local test nodes)"
    )]
    InsecureRpcUrl { chain: Chain },
    #[error(transparent)]
    Evm(#[from] st0x_evm::EvmError),
}

/// Rejects wallet RPC URLs that would carry signing traffic over cleartext
/// HTTP. HTTP is allowed only for loopback hosts, so local test nodes
/// (Anvil) keep working while any routable endpoint must be HTTPS. Enforced
/// both at config load and in [`OnchainWalletCtx::new`], so no caller of the
/// public constructor can bypass it.
pub(crate) fn require_secure_wallet_rpc_url(url: &Url, chain: Chain) -> Result<(), WalletCtxError> {
    if url.scheme() == "https" {
        return Ok(());
    }

    let is_loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        Some(url::Host::Domain(host)) => host == "localhost",
        None => false,
    };

    if is_loopback && url.scheme() == "http" {
        Ok(())
    } else {
        Err(WalletCtxError::InsecureRpcUrl { chain })
    }
}

/// Pre-built signing wallets for configured chains.
///
/// Independent of rebalancing — any trading mode can optionally
/// configure a wallet for manual CLI operations.
#[derive(Clone)]
pub struct OnchainWalletCtx {
    base: Arc<dyn Wallet<Provider = RootProvider>>,
    ethereum: Arc<dyn Wallet<Provider = RootProvider>>,
    hyperevm: Arc<dyn Wallet<Provider = RootProvider>>,
    robinhood: Option<Arc<dyn Wallet<Provider = RootProvider>>>,
}

/// What one chain's signer is built from: its endpoint and the confirmation
/// depth its submitted transactions wait for. The depth comes from that
/// chain's `[chains.<name>]` entry -- a load-balanced RPC fleet can route a
/// later request to a node that has not seen the tx, so every submission
/// waits out its own chain's reorg behaviour, not a global constant.
pub struct SigningChain {
    pub rpc_url: Url,
    pub required_confirmations: u64,
}

/// Per-chain signer parameters for every chain the wallet is built for.
///
/// Robinhood remains optional so introducing its observe-only chain identity
/// does not force deployed configurations to gain signing access before the
/// trading rollout needs it.
pub struct SigningChains {
    pub base: SigningChain,
    pub ethereum: SigningChain,
    pub hyperevm: SigningChain,
    pub robinhood: Option<SigningChain>,
}

impl SigningChains {
    /// How each signer reaches its chain RPC under `jwt`. Wallets are built
    /// before logging is up, so `st0x-rpc`'s own log lines for them are lost.
    pub(crate) fn rpc_transport_notices(&self, jwt: &JwtEnv) -> Vec<StartupNotice> {
        let chains = [
            (Chain::Base, Some(&self.base)),
            (Chain::Ethereum, Some(&self.ethereum)),
            (Chain::HyperEvm, Some(&self.hyperevm)),
            (Chain::Robinhood, self.robinhood.as_ref()),
        ];

        let mut notices: Vec<StartupNotice> = chains
            .into_iter()
            .filter_map(|(chain, signing_chain)| {
                let transport = if st0x_rpc::uses_alchemy_jwt(signing_chain?.rpc_url.as_str()) {
                    "Alchemy with a KMS-signed JWT"
                } else {
                    "plain HTTP"
                };
                Some(StartupNotice::info(format!(
                    "{chain} wallet RPC: {transport}"
                )))
            })
            .collect();

        if let JwtEnv::Partial { missing } = jwt {
            notices.push(StartupNotice::warning(format!(
                "Alchemy JWT settings are incomplete and unused: {missing} is not set"
            )));
        }

        notices
    }
}

impl OnchainWalletCtx {
    /// Build one signing wallet per chain from raw TOML config/secrets and the
    /// registry's RPC endpoints.
    ///
    /// Without wallet features, `WalletKind` is uninhabited so
    /// deserialization always fails at the `?` -- making later clones
    /// appear redundant to clippy.
    #[cfg_attr(
        not(any(feature = "wallet-turnkey", feature = "wallet-private-key")),
        allow(clippy::redundant_clone)
    )]
    pub async fn new(
        wallet_config: toml::Value,
        wallet_secrets: toml::Value,
        chains: SigningChains,
    ) -> Result<Self, WalletCtxError> {
        let SigningChains {
            base,
            ethereum,
            hyperevm,
            robinhood,
        } = chains;

        require_secure_wallet_rpc_url(&base.rpc_url, Chain::Base)?;
        require_secure_wallet_rpc_url(&ethereum.rpc_url, Chain::Ethereum)?;
        require_secure_wallet_rpc_url(&hyperevm.rpc_url, Chain::HyperEvm)?;
        if let Some(robinhood) = &robinhood {
            require_secure_wallet_rpc_url(&robinhood.rpc_url, Chain::Robinhood)?;
        }

        let WalletKindTag { kind } = WalletKindTag::deserialize(wallet_config.clone())?;

        let (base_wallet, ethereum_wallet, hyperevm_wallet) = tokio::try_join!(
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                Chain::Base,
                base.rpc_url,
                base.required_confirmations,
            ),
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                Chain::Ethereum,
                ethereum.rpc_url,
                ethereum.required_confirmations,
            ),
            build_wallet(
                &kind,
                wallet_config.clone(),
                wallet_secrets.clone(),
                Chain::HyperEvm,
                hyperevm.rpc_url,
                hyperevm.required_confirmations,
            ),
        )?;
        let robinhood_wallet = match robinhood {
            Some(robinhood) => Some(
                build_wallet(
                    &kind,
                    wallet_config,
                    wallet_secrets,
                    Chain::Robinhood,
                    robinhood.rpc_url,
                    robinhood.required_confirmations,
                )
                .await?,
            ),
            None => None,
        };

        info!(
            target: "wallet",
            wallet = %base_wallet.address(),
            robinhood = robinhood_wallet.is_some(),
            "Initialized onchain wallets"
        );

        Ok(Self {
            base: base_wallet,
            ethereum: ethereum_wallet,
            hyperevm: hyperevm_wallet,
            robinhood: robinhood_wallet,
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

    pub fn robinhood_wallet(&self) -> Option<&Arc<dyn Wallet<Provider = RootProvider>>> {
        self.robinhood.as_ref()
    }
}

/// Creates a chain RPC client with a retry layer for transient errors.
///
/// Redirects are not followed: the RPC URL is validated at config load (https
/// or loopback), and following a redirect would let the server route signing
/// traffic to a destination that never passed that validation. The request
/// timeout bounds a hung endpoint; the retry layer above it handles the
/// resulting failures.
async fn http_client_with_retry(url: &Url) -> Result<RpcClient, ChainRpcError> {
    let retry_layer = RetryBackoffLayer::new(
        RPC_MAX_RETRIES,
        RPC_INITIAL_BACKOFF_MS,
        RPC_COMPUTE_UNITS_PER_SECOND,
    );
    let timeouts = ChainRpcTimeouts {
        connect: None,
        request: Some(RPC_REQUEST_TIMEOUT),
    };

    chain_rpc_client(url, timeouts, retry_layer).await
}

pub async fn build_wallet(
    kind: &WalletKind,
    wallet_config: toml::Value,
    wallet_secrets: toml::Value,
    chain: Chain,
    rpc_url: Url,
    required_confirmations: u64,
) -> Result<Arc<dyn Wallet<Provider = RootProvider>>, WalletCtxError> {
    let rpc_client = http_client_with_retry(&rpc_url)
        .await
        .map_err(|source| WalletCtxError::ChainRpc { chain, source })?;
    let provider = RootProvider::new(rpc_client);

    Ok(kind
        .try_into_wallet(EvmWalletCtx {
            settings: TomlValue(wallet_config),
            credentials: TomlValue(wallet_secrets),
            provider,
            required_confirmations,
        })
        .await?)
}

#[cfg(any(test, feature = "test-support"))]
impl OnchainWalletCtx {
    /// Create a stub wallet context for tests.
    ///
    /// Each chain gets a distinct marker address so a test that routes
    /// through the wrong chain's wallet observes the wrong address instead
    /// of passing by coincidence.
    pub fn stub() -> Self {
        use alloy::primitives::address;

        Self {
            base: st0x_evm::StubWallet::stub(address!(
                "0x0000000000000000000000000000000000000ba5"
            )),
            ethereum: st0x_evm::StubWallet::stub(address!(
                "0x0000000000000000000000000000000000000e78"
            )),
            hyperevm: st0x_evm::StubWallet::stub(address!(
                "0x0000000000000000000000000000000000000999"
            )),
            robinhood: Some(st0x_evm::StubWallet::stub(address!(
                "0x0000000000000000000000000000000000004663"
            ))),
        }
    }

    /// Create a stub wallet context without an optional Robinhood signer.
    pub fn stub_without_robinhood() -> Self {
        Self {
            robinhood: None,
            ..Self::stub()
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
        robinhood_wallet: Arc<dyn Wallet<Provider = RootProvider>>,
    ) -> Self {
        Self {
            base: base_wallet,
            ethereum: ethereum_wallet,
            hyperevm: hyperevm_wallet,
            robinhood: Some(robinhood_wallet),
        }
    }
}

#[cfg(test)]
mod tests {
    use httpmock::{HttpMockRequest, HttpMockResponse, MockServer};
    use std::sync::atomic::{AtomicUsize, Ordering};

    use crate::loader::StartupNoticeLevel;

    use super::*;

    fn signing_chain(rpc_url: &str) -> SigningChain {
        SigningChain {
            rpc_url: rpc_url.parse().unwrap(),
            required_confirmations: 1,
        }
    }

    #[test]
    fn rpc_transport_notices_name_each_signer_transport() {
        let chains = SigningChains {
            base: signing_chain("https://base-mainnet.g.alchemy.com/v2"),
            ethereum: signing_chain("https://eth-mainnet.g.alchemy.com/v2/api-key"),
            hyperevm: signing_chain("https://rpc.hyperliquid.xyz/evm"),
            robinhood: None,
        };

        let messages: Vec<String> = chains
            .rpc_transport_notices(&JwtEnv::Unset)
            .into_iter()
            .map(|notice| {
                assert_eq!(notice.level, StartupNoticeLevel::Info);
                notice.message
            })
            .collect();

        assert_eq!(
            messages,
            [
                format!("{} wallet RPC: Alchemy with a KMS-signed JWT", Chain::Base),
                format!("{} wallet RPC: plain HTTP", Chain::Ethereum),
                format!("{} wallet RPC: plain HTTP", Chain::HyperEvm),
            ]
        );
    }

    #[test]
    fn rpc_transport_notices_warn_about_half_set_jwt_env() {
        let chains = SigningChains {
            base: signing_chain("https://rpc.example.com"),
            ethereum: signing_chain("https://rpc.example.com"),
            hyperevm: signing_chain("https://rpc.example.com"),
            robinhood: Some(signing_chain("https://rpc.example.com")),
        };

        let notices = chains.rpc_transport_notices(&JwtEnv::Partial {
            missing: st0x_rpc::KID_VAR,
        });

        assert_eq!(notices.len(), 5, "{notices:?}");
        assert_eq!(
            notices[4],
            StartupNotice::warning(
                "Alchemy JWT settings are incomplete and unused: ALCHEMY_JWT_KID is not set"
            )
        );
    }

    #[test]
    fn bare_alchemy_wallet_rpc_url_without_jwt_env_is_refused() {
        temp_env::with_vars_unset(["ALCHEMY_JWT_KMS_KEY_VERSION", "ALCHEMY_JWT_KID"], || {
            let error = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(http_client_with_retry(
                    &"https://base-mainnet.g.alchemy.com/v2".parse().unwrap(),
                ))
                .unwrap_err();

            assert!(
                matches!(error, ChainRpcError::MissingJwtConfig { .. }),
                "{error:?}"
            );
        });
    }

    #[tokio::test]
    async fn wallet_rpc_client_retries_rate_limited_calls() {
        let server = MockServer::start_async().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let responder_attempts = Arc::clone(&attempts);
        let rpc = server.mock(|when, then| {
            when.method("POST").path("/rpc");
            then.respond_with(move |_: &HttpMockRequest| {
                if responder_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    return HttpMockResponse::builder()
                        .status(429)
                        .body("rate limited")
                        .build();
                }

                HttpMockResponse::builder()
                    .status(200)
                    .header("content-type", "application/json")
                    .body(r#"{"jsonrpc":"2.0","id":0,"result":"0x2105"}"#)
                    .build()
            });
        });
        let client = http_client_with_retry(&server.url("/rpc").parse().unwrap())
            .await
            .unwrap();

        let chain_id: String = client.request_noparams("eth_chainId").await.unwrap();

        assert_eq!(chain_id, "0x2105");
        rpc.assert_calls(2);
    }
}
