//! Turnkey secure enclave transaction submission.
//!
//! `TurnkeyWallet` submits transactions via Turnkey's AWS Nitro secure
//! enclaves for low-latency signing (50-100ms). Like
//! [`RawPrivateKeyWallet`](super::local::RawPrivateKeyWallet), it wraps
//! the base provider with a [`WalletFiller`] -- the only difference is
//! the signer: `TurnkeySigner` (remote signing via Turnkey API) instead
//! of `PrivateKeySigner` (local key).

use alloy::consensus::{SignableTransaction, TxEnvelope};
use alloy::eips::eip2718::{Decodable2718, Eip2718Error};
use alloy::network::{Ethereum, EthereumWallet, TxSigner};
use alloy::primitives::{
    Address, B256, Bytes, ChainId, Signature, SignatureError, TxHash, hex, keccak256,
};
use alloy::providers::fillers::{
    BlobGasFiller, ChainIdFiller, FillProvider, GasFiller, JoinFill, NonceFiller, WalletFiller,
};
use alloy::providers::{Identity, Provider, ProviderBuilder};
use alloy::rpc::types::TransactionReceipt;
use alloy::signers::{Error as SignerError, Result as SignerResult};
use async_trait::async_trait;
use futures::lock::Mutex;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use serde::Serialize;
use std::sync::Arc;
use std::time::{Duration, SystemTime, SystemTimeError, UNIX_EPOCH};
use tracing::{info, trace};
use turnkey_api_key_stamper::{Stamp, StampHeader, TurnkeyP256ApiKey};
use turnkey_client::generated::{
    Activity, ActivityResponse, ActivityStatus, SignTransactionIntentV2, SignTransactionRequest,
    immutable::activity::v1::{SignTransactionResult, result},
    immutable::common::v1::TransactionType,
};
use turnkey_client::{RetryConfig, TurnkeyClientError};

use crate::gcp_kms_stamper::{GcpKmsStamper, GcpKmsStamperError};
use crate::inflight_nonces::InFlightNonces;
use crate::nonce::ResettableNonceManager;
use crate::submit::{release_in_flight_after_wait, send_with_recovery};
use crate::{Evm, EvmError, TryIntoWallet, Wallet, WalletCtx};

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
    Signer(#[from] TracingTurnkeySignerError),
}

/// Error from one Turnkey request round-trip: either the client/HTTP
/// layer or, for KMS-stamped requests, the stamping step itself.
#[derive(Debug, thiserror::Error)]
pub enum TurnkeyRequestError {
    #[error(transparent)]
    Client(#[from] TurnkeyClientError),
    #[error(transparent)]
    KmsStamper(#[from] GcpKmsStamperError),
}

/// Errors that can occur when using the traced Turnkey signer.
#[derive(Debug, thiserror::Error)]
pub enum TracingTurnkeySignerError {
    #[error(transparent)]
    TurnkeyClient(#[from] TurnkeyRequestError),
    #[error(
        "Turnkey credentials ambiguous: both [wallet].kms_api_key (config) and \
         api_private_key (secrets) are set -- keep exactly one"
    )]
    AmbiguousCredentials,
    #[error(
        "Turnkey credentials missing: set either [wallet].kms_api_key in config \
         (KMS-stamped, keyless) or api_private_key in [wallet] secrets"
    )]
    MissingCredentials,
    #[error("invalid hex string: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid EIP-2718 signed transaction envelope: {0}")]
    Rlp(#[from] Eip2718Error),
    #[error("failed to recover signer address from signature: {0}")]
    SignatureRecovery(#[from] SignatureError),
    #[error(
        "Turnkey-returned signature recovers to {recovered}, expected signer {expected} -- \
         refusing to trust a signature over content we did not request"
    )]
    SignerAddressMismatch {
        expected: Address,
        recovered: Address,
    },
    #[error("transaction is missing a chain id")]
    MissingTxChainId,
    #[error("system time is before UNIX epoch: {source}")]
    SystemTime { source: SystemTimeError },
}

/// Full resource name of a GCP KMS `EC_SIGN_P256_SHA256` key version.
///
/// Shaped `projects/.../cryptoKeyVersions/N`; its PUBLIC half is
/// registered as a Turnkey API user. Non-secret — it names an IAM-gated
/// key, it is not a credential itself. See [`crate::gcp_kms_stamper`].
#[derive(Debug, Clone, Deserialize)]
#[serde(transparent)]
pub struct TurnkeyKmsApiKey(String);

impl TurnkeyKmsApiKey {
    pub fn new(value: String) -> Self {
        Self(value)
    }
}

/// Non-secret Turnkey configuration: wallet address, organization ID,
/// and (for keyless KMS stamping) the KMS API-key version name.
#[derive(Debug, Clone, Deserialize)]
pub struct TurnkeySettings {
    pub address: Address,
    pub organization_id: TurnkeyOrganizationId,
    /// When set, Turnkey requests are stamped by this KMS key via the
    /// runtime's ambient GCP identity and `[wallet]` secrets need no
    /// `api_private_key` (setting both is refused as ambiguous).
    #[serde(default)]
    pub kms_api_key: Option<TurnkeyKmsApiKey>,
}

/// Secret Turnkey credential: the P-256 API private key. Absent when
/// the KMS stamper is configured ([`TurnkeySettings::kms_api_key`]).
#[derive(Clone, Default, Deserialize)]
pub struct TurnkeyCredentials {
    #[serde(default)]
    pub api_private_key: Option<TurnkeyApiPrivateKey>,
}

impl std::fmt::Debug for TurnkeyCredentials {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TurnkeyCredentials")
            .field(
                "api_private_key",
                &self.api_private_key.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// Provider type produced by wrapping a base provider with fillers
/// and a [`WalletFiller`] backed by a Turnkey signer.
///
/// Uses [`ResettableNonceManager`] instead of alloy's default
/// `CachedNonceManager` so the nonce cache can be invalidated when
/// external processes (e.g. CLI commands) advance the chain nonce.
type SignerProvider<P> = FillProvider<
    JoinFill<
        JoinFill<
            JoinFill<
                JoinFill<JoinFill<Identity, GasFiller>, BlobGasFiller>,
                NonceFiller<ResettableNonceManager>,
            >,
            ChainIdFiller,
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
    /// Shared nonce manager — cloned from the one inside
    /// `signing_provider` so we can call [`invalidate()`] on "nonce
    /// too low" errors without needing to traverse the filler chain.
    nonce_manager: ResettableNonceManager,
    /// This wallet's own record of nonces it has assigned to transactions
    /// not yet confirmed or proven dropped. Shared across clones, same as
    /// `nonce_manager`.
    in_flight: InFlightNonces,
    /// Serializes sends from this wallet so concurrent callers cannot
    /// build two transactions at the same nonce. Shared across clones so
    /// every handle to the same address contends on one lock.
    send_lock: Arc<Mutex<()>>,
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
    pub async fn new(
        ctx: WalletCtx<TurnkeySettings, TurnkeyCredentials, P>,
    ) -> Result<Self, EvmError> {
        let TurnkeySettings {
            address,
            organization_id,
            kms_api_key,
        } = ctx.settings;
        let TurnkeyCredentials { api_private_key } = ctx.credentials;

        let chain_id = ctx.provider.get_chain_id().await?;

        // Exactly one credential source: a stored P-256 API key (secrets)
        // or a KMS-held one addressed from config (keyless). Both set is
        // ambiguous and refused rather than silently preferring one.
        let stamper = match (kms_api_key, api_private_key) {
            (Some(TurnkeyKmsApiKey(key_version)), None) => ApiStamper::GcpKms(
                GcpKmsStamper::new(key_version)
                    .await
                    .map_err(|error| TurnkeyError::Signer(error.into()))?,
            ),
            (None, Some(api_private_key)) => ApiStamper::local(&api_private_key)
                .map_err(|error| TurnkeyError::Signer(error.into()))?,
            (Some(_), Some(_)) => {
                return Err(
                    TurnkeyError::Signer(TracingTurnkeySignerError::AmbiguousCredentials).into(),
                );
            }
            (None, None) => {
                return Err(
                    TurnkeyError::Signer(TracingTurnkeySignerError::MissingCredentials).into(),
                );
            }
        };

        let client = TracingTurnkeyClient::for_stamper(stamper)
            .map_err(|error| TurnkeyError::Signer(error.into()))?;
        let signer = TracingTurnkeySigner::new(client, organization_id, address, Some(chain_id));

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = ctx.provider.clone();
        let nonce_manager = ResettableNonceManager::default();

        let signing_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .filler(GasFiller)
            .filler(BlobGasFiller::default())
            .with_nonce_management(nonce_manager.clone())
            .filler(ChainIdFiller::default())
            .wallet(eth_wallet)
            .connect_provider(ctx.provider);

        let required_confirmations = ctx.required_confirmations;

        info!(target: "wallet", %address, required_confirmations, "Turnkey wallet initialized");

        Ok(Self {
            provider: base_provider,
            signing_provider,
            nonce_manager,
            in_flight: InFlightNonces::default(),
            send_lock: Arc::new(Mutex::new(())),
            address,
            required_confirmations,
        })
    }

    /// Creates a `TurnkeyWallet` from a pre-built `TurnkeyClient`.
    ///
    /// Used in tests to inject a client configured with a mock server
    /// base URL. Production code should use [`new`](Self::new).
    #[cfg(test)]
    async fn from_client(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        provider: P,
        required_confirmations: u64,
    ) -> Result<Self, EvmError> {
        let chain_id = provider.get_chain_id().await?;

        let signer = TracingTurnkeySigner::new(client, organization_id, address, Some(chain_id));

        let eth_wallet = EthereumWallet::from(signer);

        let base_provider = provider.clone();
        let nonce_manager = ResettableNonceManager::default();

        let signing_provider = ProviderBuilder::new()
            .disable_recommended_fillers()
            .filler(GasFiller)
            .filler(BlobGasFiller::default())
            .with_nonce_management(nonce_manager.clone())
            .filler(ChainIdFiller::default())
            .wallet(eth_wallet)
            .connect_provider(provider);

        Ok(Self {
            provider: base_provider,
            signing_provider,
            nonce_manager,
            in_flight: InFlightNonces::default(),
            send_lock: Arc::new(Mutex::new(())),
            address,
            required_confirmations,
        })
    }
}

impl From<GcpKmsStamperError> for TracingTurnkeySignerError {
    fn from(error: GcpKmsStamperError) -> Self {
        Self::TurnkeyClient(TurnkeyRequestError::KmsStamper(error))
    }
}

impl From<TurnkeyClientError> for TracingTurnkeySignerError {
    fn from(error: TurnkeyClientError) -> Self {
        Self::TurnkeyClient(TurnkeyRequestError::Client(error))
    }
}

/// How outgoing Turnkey requests are stamped: with a locally-held P-256
/// API private key (the classic stored credential) or via a GCP
/// KMS-held key signed through the runtime's ambient IAM identity
/// (keyless — see [`crate::gcp_kms_stamper`]).
enum ApiStamper {
    Local(TurnkeyP256ApiKey),
    GcpKms(GcpKmsStamper),
}

impl std::fmt::Debug for ApiStamper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local(_) => formatter.write_str("ApiStamper::Local([REDACTED])"),
            Self::GcpKms(stamper) => formatter
                .debug_tuple("ApiStamper::GcpKms")
                .field(stamper)
                .finish(),
        }
    }
}

impl ApiStamper {
    fn local(api_private_key: &TurnkeyApiPrivateKey) -> Result<Self, TurnkeyClientError> {
        let TurnkeyApiPrivateKey(api_key_hex) = api_private_key;
        Ok(Self::Local(TurnkeyP256ApiKey::from_strings(
            api_key_hex,
            None,
        )?))
    }

    async fn stamp(&self, body: &[u8]) -> Result<StampHeader, TurnkeyRequestError> {
        match self {
            Self::Local(api_key) => api_key
                .stamp(body)
                .map_err(|error| TurnkeyRequestError::Client(error.into())),
            Self::GcpKms(stamper) => Ok(stamper.stamp(body).await?),
        }
    }
}

struct TracingTurnkeyClient {
    http: reqwest::Client,
    base_url: String,
    stamper: ApiStamper,
    retry_config: RetryConfig,
}

impl std::fmt::Debug for TracingTurnkeyClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeyClient")
            .field("base_url", &self.base_url)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeyClient {
    fn for_stamper(stamper: ApiStamper) -> Result<Self, TurnkeyClientError> {
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            "https://api.turnkey.com".to_string(),
            stamper,
            RetryConfig::default(),
        ))
    }

    #[cfg(test)]
    fn for_base_url(
        base_url: String,
        api_key: TurnkeyP256ApiKey,
    ) -> Result<Self, TurnkeyClientError> {
        Ok(Self::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .user_agent("st0x-turnkey-client")
                .build()
                .map_err(TurnkeyClientError::ReqwestBuilder)?,
            base_url,
            ApiStamper::Local(api_key),
            RetryConfig::default(),
        ))
    }

    fn new(
        http: reqwest::Client,
        base_url: String,
        stamper: ApiStamper,
        retry_config: RetryConfig,
    ) -> Self {
        Self {
            http,
            base_url,
            stamper,
            retry_config,
        }
    }

    fn current_timestamp() -> Result<u128, TracingTurnkeySignerError> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| TracingTurnkeySignerError::SystemTime { source })
    }

    async fn sign_transaction(
        &self,
        organization_id: TurnkeyOrganizationId,
        timestamp_ms: u128,
        params: SignTransactionIntentV2,
    ) -> Result<SignTransactionResult, TurnkeyRequestError> {
        let TurnkeyOrganizationId(organization_id) = organization_id;
        let request = SignTransactionRequest {
            r#type: "ACTIVITY_TYPE_SIGN_TRANSACTION_V2".to_string(),
            timestamp_ms: timestamp_ms.to_string(),
            parameters: Some(params),
            organization_id,
        };
        let activity = self
            .process_activity(&request, "/public/v1/submit/sign_transaction")
            .await?;
        let inner = activity
            .result
            .ok_or(TurnkeyClientError::MissingResult)?
            .inner
            .ok_or(TurnkeyClientError::MissingInnerResult)?;

        match inner {
            result::Inner::SignTransactionResult(result) => Ok(result),
            other => Err(TurnkeyClientError::UnexpectedInnerActivityResult(
                serde_json::to_string(&other).map_err(TurnkeyClientError::from)?,
            )
            .into()),
        }
    }

    async fn process_activity<Request: Serialize + Sync>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Activity, TurnkeyRequestError> {
        let mut retry_count = 0;

        loop {
            let response: ActivityResponse = self.process_request(request, path).await?;
            let activity = response
                .activity
                .ok_or(TurnkeyClientError::MissingActivity)?;

            match activity.status {
                ActivityStatus::Completed => return Ok(activity),
                ActivityStatus::Pending => {
                    if retry_count >= self.retry_config.max_retries {
                        return Err(TurnkeyClientError::ExceededRetries(retry_count).into());
                    }

                    retry_count += 1;
                    tokio::time::sleep(self.retry_config.compute_delay(retry_count)).await;
                }
                ActivityStatus::Failed => {
                    return Err(TurnkeyClientError::ActivityFailed(activity.failure).into());
                }
                ActivityStatus::ConsensusNeeded => {
                    return Err(TurnkeyClientError::ActivityRequiresApproval(activity.id).into());
                }
                ActivityStatus::Unspecified
                | ActivityStatus::Created
                | ActivityStatus::Rejected => {
                    return Err(TurnkeyClientError::UnexpectedActivityStatus(
                        activity.status.as_str_name().to_string(),
                    )
                    .into());
                }
            }
        }
    }

    /// Serializes the request, stamps its exact bytes (locally or via
    /// KMS), and dispatches — the split exists because stamping can now
    /// fail with a non-client error ([`GcpKmsStamperError`]).
    async fn process_request<Request, Response>(
        &self,
        request: &Request,
        path: &str,
    ) -> Result<Response, TurnkeyRequestError>
    where
        Request: Serialize + Sync,
        Response: serde::de::DeserializeOwned,
    {
        let post_body = serde_json::to_string(request).map_err(TurnkeyClientError::from)?;
        let stamp = self.stamper.stamp(post_body.as_bytes()).await?;
        Ok(self.dispatch(path, stamp, post_body).await?)
    }

    async fn dispatch<Response>(
        &self,
        path: &str,
        StampHeader { name, value }: StampHeader,
        post_body: String,
    ) -> Result<Response, TurnkeyClientError>
    where
        Response: serde::de::DeserializeOwned,
    {
        let url = format!("{}{}", self.base_url, path);
        let response = self
            .http
            .post(&url)
            .header(name, value)
            .body(post_body)
            .send()
            .await?;
        let status = response.status();
        let content_type = response.headers().get(CONTENT_TYPE).cloned();
        // Read raw bytes and parse the success body with `serde_json::from_slice`
        // so invalid UTF-8 fails fast, matching the fail-fast convention the
        // Alpaca clients follow. Lossy decoding is used only for the
        // error-body display.
        let bytes = response.bytes().await?;

        // The successful `sign_transaction` body contains a fully-signed,
        // broadcastable RLP-encoded transaction -- a bearer instrument until
        // the bot broadcasts it. Only request metadata is logged here, never
        // the body, so trace-level "wallet" logs cannot be used to
        // front-run or replay the transaction.
        trace!(
            target: "wallet",
            method = "POST",
            status = %status,
            url = %url,
            "Turnkey API response received"
        );

        // Error mapping below reproduces upstream `turnkey_client`'s handling
        // (including its String-valued error variants); it is not a new
        // stringly-typed-error introduction. This client is a deliberate fork
        // of `turnkey_client` to trace request metadata above (and preserve
        // fail-fast body parsing) without ever logging the response body.
        let content_type = content_type
            .ok_or(TurnkeyClientError::MissingContentTypeHeader)?
            .to_str()
            .map_err(|error| TurnkeyClientError::HeaderToStrError(error.to_string()))?
            .parse::<mime::Mime>()
            .map_err(|error| TurnkeyClientError::HeaderFromStrError(error.to_string()))?;

        if !status.is_success() {
            return Err(TurnkeyClientError::UnexpectedHttpStatus(
                status.as_u16(),
                String::from_utf8_lossy(&bytes).into_owned(),
            ));
        }

        // Compare only the MIME essence (type/subtype) so responses that carry
        // parameters such as `application/json; charset=utf-8` are accepted;
        // strict equality against `mime::APPLICATION_JSON` rejects them.
        if content_type.essence_str() != mime::APPLICATION_JSON.essence_str() {
            return Err(TurnkeyClientError::UnexpectedMimeType(
                content_type.to_string(),
            ));
        }

        // This path fires on a 2xx response whose JSON fails to deserialize --
        // for `sign_transaction` that body still contains the fully-signed
        // transaction (a bearer instrument), so it must not be embedded in
        // the error. The byte count preserves debuggability without leaking
        // the payload; `error` (a `serde_json::Error`) still carries the
        // parse location.
        serde_json::from_slice(&bytes).map_err(|error| {
            TurnkeyClientError::Decode(
                format!(
                    "<{} bytes redacted: response body may contain a signed transaction>",
                    bytes.len()
                ),
                error,
            )
        })
    }
}

struct TracingTurnkeySigner {
    client: TracingTurnkeyClient,
    organization_id: TurnkeyOrganizationId,
    address: Address,
    chain_id: Option<ChainId>,
}

impl std::fmt::Debug for TracingTurnkeySigner {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TracingTurnkeySigner")
            .field("organization_id", &self.organization_id)
            .field("address", &self.address)
            .field("chain_id", &self.chain_id)
            .finish_non_exhaustive()
    }
}

impl TracingTurnkeySigner {
    fn new(
        client: TracingTurnkeyClient,
        organization_id: TurnkeyOrganizationId,
        address: Address,
        chain_id: Option<ChainId>,
    ) -> Self {
        Self {
            client,
            organization_id,
            address,
            chain_id,
        }
    }

    /// Decodes Turnkey's signed-transaction envelope and extracts its
    /// signature, verifying that the signature actually recovers to
    /// `expected_signer` over `expected_hash`.
    ///
    /// Turnkey returns a full signed RLP envelope rather than raw r/s/v
    /// components; alloy's `TxSigner` contract only needs the
    /// `Signature` (the caller re-encodes the transaction using it), so
    /// byte-for-byte re-encoding is not required. What matters is that
    /// the returned signature is genuinely over the exact bytes we
    /// asked Turnkey to sign, by the address we expect -- a defensive
    /// check that turns "trust Turnkey's blob" into "cryptographically
    /// verify the blob signs what we asked, by us."
    fn parse_signature(
        response: &SignTransactionResult,
        expected_hash: B256,
        expected_signer: Address,
    ) -> Result<Signature, TracingTurnkeySignerError> {
        let signed_bytes = hex::decode(&response.signed_transaction)?;
        let envelope = TxEnvelope::decode_2718_exact(&signed_bytes)?;
        let signature = *envelope.signature();
        let recovered = signature.recover_address_from_prehash(&expected_hash)?;

        if recovered != expected_signer {
            return Err(TracingTurnkeySignerError::SignerAddressMismatch {
                expected: expected_signer,
                recovered,
            });
        }

        Ok(signature)
    }
}

#[async_trait]
impl TxSigner<Signature> for TracingTurnkeySigner {
    fn address(&self) -> Address {
        self.address
    }

    // Only legacy (EIP-155) and EIP-1559 transactions are exercised in
    // production (the bot's fill pipeline never constructs EIP-4844 blob
    // or EIP-7702 set-code transactions). `encoded_for_signing()` handles
    // every `SignableTransaction` variant regardless, and the
    // recovered-address check below fails closed on any Turnkey
    // mishandling of an unexercised variant, so no explicit reject arm
    // is added here.
    async fn sign_transaction(
        &self,
        tx: &mut dyn SignableTransaction<Signature>,
    ) -> SignerResult<Signature> {
        if let Some(chain_id) = self.chain_id
            && !tx.set_chain_id_checked(chain_id)
        {
            // `set_chain_id_checked` only returns false when the tx already
            // carries a (mismatching) chain id, so `tx.chain_id()` is always
            // Some here. The `MissingTxChainId` fallback is defensive (avoids
            // a panic the no-unwrap rule forbids) and is not expected to fire.
            let tx_chain_id = tx
                .chain_id()
                .ok_or_else(|| SignerError::other(TracingTurnkeySignerError::MissingTxChainId))?;
            return Err(SignerError::TransactionChainIdMismatch {
                signer: chain_id,
                tx: tx_chain_id,
            });
        }

        // Hash the exact bytes transmitted to Turnkey (not a separate
        // `tx.signature_hash()` call) so the recovered-address check below
        // proves the returned signature signs the payload we actually sent,
        // even though `signature_hash()` is overridable on the trait object.
        let unsigned_rlp = tx.encoded_for_signing();
        let expected_hash = keccak256(&unsigned_rlp);

        let response = self
            .client
            .sign_transaction(
                self.organization_id.clone(),
                TracingTurnkeyClient::current_timestamp().map_err(SignerError::other)?,
                SignTransactionIntentV2 {
                    sign_with: self.address.to_string(),
                    unsigned_transaction: hex::encode(&unsigned_rlp),
                    r#type: TransactionType::Ethereum,
                },
            )
            .await
            .map_err(|error| SignerError::other(TracingTurnkeySignerError::TurnkeyClient(error)))?;

        Self::parse_signature(&response, expected_hash, self.address).map_err(SignerError::other)
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

    async fn send_pending(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TxHash, EvmError> {
        info!(target: "wallet", %contract, note, "Submitting Turnkey contract call");

        send_with_recovery(
            &self.signing_provider,
            &self.nonce_manager,
            &self.in_flight,
            &self.send_lock,
            self.address,
            contract,
            calldata,
            note,
        )
        .await
    }

    async fn await_receipt(&self, tx_hash: TxHash) -> Result<TransactionReceipt, EvmError> {
        let result =
            crate::wait_for_receipt(&self.provider, tx_hash, self.required_confirmations).await;

        release_in_flight_after_wait(&self.in_flight, self.address, tx_hash, &result);

        let receipt = result?;

        info!(target: "wallet", tx_hash = %receipt.transaction_hash, "Transaction confirmed");

        Ok(receipt)
    }

    async fn send(
        &self,
        contract: Address,
        calldata: Bytes,
        note: &str,
    ) -> Result<TransactionReceipt, EvmError> {
        let tx_hash = self.send_pending(contract, calldata, note).await?;
        self.await_receipt(tx_hash).await
    }
}

#[async_trait]
impl<P> TryIntoWallet for TurnkeyWallet<P>
where
    P: Provider + Clone + Send + Sync + 'static,
{
    type Settings = TurnkeySettings;
    type Credentials = TurnkeyCredentials;

    async fn try_from_ctx(
        ctx: WalletCtx<TurnkeySettings, TurnkeyCredentials, P>,
    ) -> Result<Self, EvmError> {
        Self::new(ctx).await
    }
}

#[cfg(test)]
mod tests {
    use alloy::consensus::{TxEip1559, TxLegacy};
    use alloy::eips::eip2718::Encodable2718;
    use alloy::eips::eip2930::AccessList;
    use alloy::node_bindings::{Anvil, AnvilInstance};
    use alloy::primitives::{TxKind, U256};
    use alloy::providers::ext::AnvilApi;
    use alloy::signers::local::PrivateKeySigner;
    use httpmock::MockServer;

    use super::*;

    /// Generate a fresh P-256 API key for testing.
    fn test_api_key() -> TurnkeyP256ApiKey {
        TurnkeyP256ApiKey::generate()
    }

    /// Build a Turnkey client that sends requests to the mock server.
    fn mock_client(server: &MockServer) -> TracingTurnkeyClient {
        TracingTurnkeyClient::for_base_url(server.base_url(), test_api_key()).unwrap()
    }

    /// Build a Turnkey client with a custom retry config so the retry
    /// loop can be exercised without real backoff delays.
    fn mock_client_with_retry(
        server: &MockServer,
        retry_config: RetryConfig,
    ) -> TracingTurnkeyClient {
        TracingTurnkeyClient::new(
            reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .unwrap(),
            server.base_url(),
            ApiStamper::Local(test_api_key()),
            retry_config,
        )
    }

    /// Builds a well-formed EIP-1559 transaction (the only shape the bot's
    /// fill pipeline emits in production) with the given chain id and
    /// destination address.
    fn eip1559_tx(chain_id: ChainId, to: Address) -> TxEip1559 {
        TxEip1559 {
            chain_id,
            nonce: 7,
            gas_limit: 21_000,
            max_fee_per_gas: 30_000_000_000,
            max_priority_fee_per_gas: 1_000_000_000,
            to: TxKind::Call(to),
            value: U256::ZERO,
            access_list: AccessList::default(),
            input: Bytes::new(),
        }
    }

    /// Builds a well-formed legacy (EIP-155) transaction with the given
    /// chain id and destination address.
    fn legacy_tx(chain_id: Option<ChainId>, to: Address) -> TxLegacy {
        TxLegacy {
            chain_id,
            nonce: 3,
            gas_price: 1_000_000_000,
            gas_limit: 21_000,
            to: TxKind::Call(to),
            value: U256::ZERO,
            input: Bytes::new(),
        }
    }

    /// Signs `tx` locally with `signer` and returns the hex-encoded
    /// EIP-2718 signed envelope, exactly as Turnkey's `signed_transaction`
    /// response field would carry it.
    async fn locally_signed_envelope_hex<Tx>(signer: &PrivateKeySigner, mut tx: Tx) -> String
    where
        Tx: SignableTransaction<Signature> + Clone,
        TxEnvelope: From<alloy::consensus::Signed<Tx>>,
    {
        let signature = signer.sign_transaction(&mut tx).await.unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        hex::encode(envelope.encoded_2718())
    }

    /// JSON body for a COMPLETED `sign_transaction` activity carrying the
    /// given signed transaction, shaped like a real Turnkey response.
    fn completed_sign_transaction_body(signed_transaction_hex: &str) -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_COMPLETED",
                "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                "fingerprint": "fingerprint",
                "result": {
                    "signTransactionResult": { "signedTransaction": signed_transaction_hex }
                }
            }
        })
    }

    /// JSON body for a PENDING activity (no result yet).
    fn pending_activity_body() -> serde_json::Value {
        serde_json::json!({
            "activity": {
                "id": "activity-id",
                "organizationId": "org-test",
                "status": "ACTIVITY_STATUS_PENDING",
                "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                "fingerprint": "fingerprint"
            }
        })
    }

    #[tokio::test]
    async fn parse_signature_decodes_eip1559_transaction() {
        let key_signer = PrivateKeySigner::random();
        let mut tx = eip1559_tx(8453, Address::random());
        let expected_hash = tx.signature_hash();

        let signature = key_signer.sign_transaction(&mut tx).await.unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let response = SignTransactionResult {
            signed_transaction: hex::encode(envelope.encoded_2718()),
        };

        let recovered =
            TracingTurnkeySigner::parse_signature(&response, expected_hash, key_signer.address())
                .unwrap();

        assert_eq!(recovered, signature);
    }

    #[tokio::test]
    async fn parse_signature_decodes_legacy_transaction() {
        let key_signer = PrivateKeySigner::random();
        let mut tx = legacy_tx(Some(8453), Address::random());
        let expected_hash = tx.signature_hash();

        let signature = key_signer.sign_transaction(&mut tx).await.unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let response = SignTransactionResult {
            signed_transaction: hex::encode(envelope.encoded_2718()),
        };

        let recovered =
            TracingTurnkeySigner::parse_signature(&response, expected_hash, key_signer.address())
                .unwrap();

        assert_eq!(recovered, signature);
    }

    #[test]
    fn parse_signature_rejects_invalid_hex() {
        let response = SignTransactionResult {
            signed_transaction: "not-hex".to_string(),
        };

        let error = TracingTurnkeySigner::parse_signature(&response, B256::ZERO, Address::random())
            .unwrap_err();

        assert!(matches!(error, TracingTurnkeySignerError::Hex(_)));
    }

    #[tokio::test]
    async fn parse_signature_rejects_truncated_rlp() {
        let key_signer = PrivateKeySigner::random();
        let mut tx = eip1559_tx(8453, Address::random());
        let expected_hash = tx.signature_hash();

        let signature = key_signer.sign_transaction(&mut tx).await.unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let mut encoded = envelope.encoded_2718();
        // Append trailing garbage: `decode_2718_exact` must reject bytes left
        // over after decoding, not just successfully decode a prefix.
        encoded.push(0xff);
        let response = SignTransactionResult {
            signed_transaction: hex::encode(encoded),
        };

        let error =
            TracingTurnkeySigner::parse_signature(&response, expected_hash, key_signer.address())
                .unwrap_err();

        assert!(matches!(error, TracingTurnkeySignerError::Rlp(_)));
    }

    #[tokio::test]
    async fn parse_signature_rejects_unrecoverable_signature() {
        let tx = eip1559_tx(8453, Address::random());
        let expected_hash = tx.signature_hash();

        // `r = 0` is not a valid ECDSA scalar, so `Signature::to_k256` fails
        // before recovery is even attempted. RLP encoding does not validate
        // signature values, so the envelope still decodes cleanly -- this is
        // the smallest construction that reaches the
        // `recover_address_from_prehash(...)?` error path without going
        // through truncated/invalid RLP (already covered separately).
        let unrecoverable_signature = Signature::new(U256::ZERO, U256::from(1), false);
        let envelope = TxEnvelope::from(tx.into_signed(unrecoverable_signature));
        let response = SignTransactionResult {
            signed_transaction: hex::encode(envelope.encoded_2718()),
        };

        let error =
            TracingTurnkeySigner::parse_signature(&response, expected_hash, Address::random())
                .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::SignatureRecovery(_)
        ));
    }

    #[tokio::test]
    async fn parse_signature_rejects_signer_mismatch() {
        let key_signer = PrivateKeySigner::random();
        let different_signer = PrivateKeySigner::random();
        let mut tx = eip1559_tx(8453, Address::random());
        let expected_hash = tx.signature_hash();

        let signature = key_signer.sign_transaction(&mut tx).await.unwrap();
        let envelope = TxEnvelope::from(tx.into_signed(signature));
        let response = SignTransactionResult {
            signed_transaction: hex::encode(envelope.encoded_2718()),
        };

        let error = TracingTurnkeySigner::parse_signature(
            &response,
            expected_hash,
            different_signer.address(),
        )
        .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::SignerAddressMismatch {
                expected,
                recovered,
            } if expected == different_signer.address() && recovered == key_signer.address()
        ));
    }

    #[tokio::test]
    async fn parse_signature_rejects_signature_over_different_content() {
        let key_signer = PrivateKeySigner::random();
        let destination_a = Address::random();
        let destination_b = Address::random();
        assert_ne!(destination_a, destination_b);

        // Two distinct transactions signed by the SAME key: `tx_for_a` is what
        // we ask Turnkey to sign, `tx_for_b` stands in for a signature Turnkey
        // (or a compromised transport) returns over different content -- the
        // headline defense `parse_signature` provides is rejecting that even
        // when the recovered key is genuinely ours.
        let tx_for_a = eip1559_tx(8453, destination_a);
        let mut tx_for_b = eip1559_tx(8453, destination_b);
        let expected_hash = keccak256(tx_for_a.encoded_for_signing());

        let signature_over_tx_for_b = key_signer.sign_transaction(&mut tx_for_b).await.unwrap();
        let envelope_over_tx_for_b =
            TxEnvelope::from(tx_for_b.into_signed(signature_over_tx_for_b));
        let response = SignTransactionResult {
            signed_transaction: hex::encode(envelope_over_tx_for_b.encoded_2718()),
        };

        let error =
            TracingTurnkeySigner::parse_signature(&response, expected_hash, key_signer.address())
                .unwrap_err();

        assert!(matches!(
            error,
            TracingTurnkeySignerError::SignerAddressMismatch { expected, recovered }
                if expected == key_signer.address() && recovered != key_signer.address()
        ));
    }

    #[tokio::test]
    async fn sign_transaction_returns_signature_from_completed_activity() {
        let key_signer = PrivateKeySigner::random();
        let address = key_signer.address();
        let chain_id: ChainId = 8453;
        let to = Address::random();

        // `chain_id` is already `Some(_)` so the signer's guard does not
        // mutate `tx` under the call -- the expected `unsignedTransaction`
        // hex below must match the chain-id state actually encoded.
        let mut tx = eip1559_tx(chain_id, to);
        let expected_unsigned_rlp = tx.encoded_for_signing();

        let signed_transaction_hex =
            locally_signed_envelope_hex(&key_signer, eip1559_tx(chain_id, to)).await;

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction")
                .json_body_includes(
                    serde_json::json!({
                        "type": "ACTIVITY_TYPE_SIGN_TRANSACTION_V2",
                        "parameters": {
                            "signWith": address.to_string(),
                            "unsignedTransaction": hex::encode(&expected_unsigned_rlp),
                            "type": "TRANSACTION_TYPE_ETHEREUM",
                        }
                    })
                    .to_string(),
                );
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_transaction_body(&signed_transaction_hex));
        });

        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            address,
            Some(chain_id),
        );

        TxSigner::sign_transaction(&signer, &mut tx).await.unwrap();

        mock.assert();
        assert_eq!(
            hex::encode(tx.encoded_for_signing()),
            hex::encode(&expected_unsigned_rlp)
        );
    }

    #[tokio::test]
    async fn sign_transaction_sets_missing_chain_id_before_encoding() {
        let key_signer = PrivateKeySigner::random();
        let address = key_signer.address();
        let chain_id: ChainId = 8453;
        let to = Address::random();

        // The signer's guard must set the chain id on `tx` before encoding,
        // so the "expected" unsigned RLP is computed from a tx that already
        // carries the signer's chain id.
        let expected_unsigned_rlp = legacy_tx(Some(chain_id), to).encoded_for_signing();
        let signed_transaction_hex =
            locally_signed_envelope_hex(&key_signer, legacy_tx(Some(chain_id), to)).await;

        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction")
                .json_body_includes(
                    serde_json::json!({
                        "parameters": {
                            "unsignedTransaction": hex::encode(&expected_unsigned_rlp),
                        }
                    })
                    .to_string(),
                );
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(completed_sign_transaction_body(&signed_transaction_hex));
        });

        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            address,
            Some(chain_id),
        );

        let mut tx_missing_chain_id = legacy_tx(None, to);

        TxSigner::sign_transaction(&signer, &mut tx_missing_chain_id)
            .await
            .unwrap();

        mock.assert();
        assert_eq!(tx_missing_chain_id.chain_id, Some(chain_id));
    }

    #[tokio::test]
    async fn sign_transaction_rejects_mismatched_chain_id_without_http_call() {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });

        let signer = TracingTurnkeySigner::new(
            mock_client(&server),
            TurnkeyOrganizationId::new("org-test".to_string()),
            Address::random(),
            Some(1),
        );

        let mut tx = legacy_tx(Some(999), Address::random());

        let error = TxSigner::sign_transaction(&signer, &mut tx)
            .await
            .unwrap_err();

        mock.assert_calls(0);
        assert!(matches!(
            error,
            SignerError::TransactionChainIdMismatch { signer: 1, tx: 999 }
        ));
    }

    #[tokio::test]
    async fn sign_transaction_exhausts_retries_while_activity_stays_pending() {
        let server = MockServer::start();

        let mock = server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(pending_activity_body());
        });

        // `RetryConfig::none()` caps retries at zero, so the first PENDING
        // response immediately exhausts retries with no backoff sleep.
        let client = mock_client_with_retry(&server, RetryConfig::none());

        let error = client
            .sign_transaction(
                TurnkeyOrganizationId::new("org-test".to_string()),
                0,
                SignTransactionIntentV2 {
                    sign_with: Address::random().to_string(),
                    unsigned_transaction: hex::encode(B256::ZERO),
                    r#type: TransactionType::Ethereum,
                },
            )
            .await
            .unwrap_err();

        mock.assert();
        assert!(matches!(
            error,
            TurnkeyRequestError::Client(TurnkeyClientError::ExceededRetries(0))
        ));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn send_signing_failure() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST")
                .path("/public/v1/submit/sign_transaction");
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
            TurnkeyOrganizationId::new("org-test".to_string()),
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
        assert!(logs_contain("Turnkey API response received"));
        assert!(logs_contain("internal server error"));
    }

    #[tracing_test::traced_test]
    #[tokio::test]
    async fn process_request_does_not_log_response_body() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "turnkey_marker": "success-body"
                }));
        });

        let client = mock_client(&server);
        let response: serde_json::Value = client
            .process_request(&serde_json::json!({"request": "body"}), "/public/v1/test")
            .await
            .unwrap();

        assert_eq!(response["turnkey_marker"], "success-body");
        assert!(logs_contain("Turnkey API response received"));
        assert!(logs_contain("/public/v1/test"));
        assert!(!logs_contain("turnkey_marker"));
        assert!(!logs_contain("success-body"));
    }

    #[tokio::test]
    async fn process_request_accepts_json_content_type_with_charset_parameter() {
        let server = MockServer::start();

        // A `Content-Type` carrying a `charset` parameter must still be accepted:
        // the essence (`application/json`) is what matters, not exact equality.
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json; charset=utf-8")
                .json_body(serde_json::json!({ "turnkey_marker": "ok" }));
        });

        let client = mock_client(&server);
        let response: serde_json::Value = client
            .process_request(&serde_json::json!({"request": "body"}), "/public/v1/test")
            .await
            .unwrap();

        assert_eq!(response["turnkey_marker"], "ok");
    }

    #[tokio::test]
    async fn process_request_rejects_non_json_content_type() {
        let server = MockServer::start();

        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "text/html")
                .body("<html></html>");
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, serde_json::Value>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();

        assert!(matches!(
            error,
            TurnkeyRequestError::Client(TurnkeyClientError::UnexpectedMimeType(mime)) if mime == "text/html"
        ));
    }

    #[tokio::test]
    async fn process_request_redacts_body_on_decode_error() {
        let server = MockServer::start();

        // Valid JSON that does not deserialize into the requested `String`
        // response type (an object is not a string), carrying a sentinel that
        // must never surface in the error -- this is the shape a signed
        // transaction body would take if it failed to decode.
        server.mock(|when, then| {
            when.method("POST").path("/public/v1/test");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "signedTransaction": "sensitive-signed-tx-marker"
                }));
        });

        let client = mock_client(&server);
        let error = client
            .process_request::<_, String>(
                &serde_json::json!({"request": "body"}),
                "/public/v1/test",
            )
            .await
            .unwrap_err();

        let TurnkeyRequestError::Client(TurnkeyClientError::Decode(message, _)) = error else {
            panic!("expected Decode error, got: {error:?}");
        };
        assert!(
            message.contains("redacted"),
            "expected redaction marker in message, got: {message}"
        );
        assert!(
            !message.contains("sensitive-signed-tx-marker"),
            "response body leaked into error message: {message}"
        );
    }

    /// Builds a WalletCtx in the shape the DigitalOcean deployments use
    /// TODAY: a stored P-256 api_private_key in [wallet] secrets, no
    /// kms_api_key anywhere.
    fn stored_key_ctx<P: Provider>(
        provider: P,
    ) -> WalletCtx<TurnkeySettings, TurnkeyCredentials, P> {
        WalletCtx {
            settings: TurnkeySettings {
                address: Address::random(),
                organization_id: TurnkeyOrganizationId::new("org-test".to_string()),
                kms_api_key: None,
            },
            credentials: TurnkeyCredentials {
                // A random P-256 scalar, hex-encoded — the exact shape the
                // secrets file carries. (turnkey_api_key_stamper 0.4 has no
                // private-key accessor on its generated keys, so mint one
                // directly with p256.)
                api_private_key: Some(TurnkeyApiPrivateKey::new(hex::encode(
                    p256::ecdsa::SigningKey::random(&mut p256::elliptic_curve::rand_core::OsRng)
                        .to_bytes(),
                ))),
            },
            provider,
            required_confirmations: 1,
        }
    }

    /// BACKWARD COMPATIBILITY: the live DO prod/staging shape -- stored
    /// api_private_key, no kms_api_key -- must keep constructing a wallet
    /// exactly as before. The KMS stamper is strictly opt-in; this test
    /// fails if it ever becomes required.
    #[tokio::test]
    async fn new_accepts_stored_api_key_without_kms() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let ctx = stored_key_ctx(provider);
        let expected_address = ctx.settings.address;

        let wallet = TurnkeyWallet::new(ctx)
            .await
            .expect("stored-api-key (DigitalOcean) wallets must keep working");

        assert_eq!(wallet.address(), expected_address);
    }

    /// A config carrying BOTH credential sources is refused rather than
    /// silently preferring one -- an operator mid-migration must be told,
    /// not guessed at. Checked before any KMS call, so no network needed.
    #[tokio::test]
    async fn new_rejects_both_credential_sources() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let mut ctx = stored_key_ctx(provider);
        ctx.settings.kms_api_key = Some(TurnkeyKmsApiKey::new(
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1".to_string(),
        ));

        let error = TurnkeyWallet::new(ctx)
            .await
            .expect_err("both credential sources must be refused");

        assert!(
            matches!(
                error,
                EvmError::Turnkey(TurnkeyError::Signer(
                    TracingTurnkeySignerError::AmbiguousCredentials
                ))
            ),
            "expected AmbiguousCredentials, got: {error:?}"
        );
    }

    /// Neither source configured fails at startup with an actionable
    /// message instead of a confusing auth failure on the first signature.
    #[tokio::test]
    async fn new_rejects_missing_credentials() {
        let anvil = Anvil::new().spawn();
        let provider = ProviderBuilder::new().connect_http(anvil.endpoint_url());

        let mut ctx = stored_key_ctx(provider);
        ctx.credentials.api_private_key = None;

        let error = TurnkeyWallet::new(ctx)
            .await
            .expect_err("no credential source must be refused");

        assert!(
            matches!(
                error,
                EvmError::Turnkey(TurnkeyError::Signer(
                    TracingTurnkeySignerError::MissingCredentials
                ))
            ),
            "expected MissingCredentials, got: {error:?}"
        );
    }

    /// BACKWARD COMPATIBILITY: the exact [wallet] TOML the DO prod and
    /// staging configs ship (no kms_api_key key at all) still
    /// deserializes -- the new field is additive and defaulted.
    #[test]
    fn existing_wallet_config_toml_still_deserializes() {
        let settings: TurnkeySettings = toml::from_str(
            r#"
            address = "0x16ca08a5825612aAe805D172a81B1a52b43574bf"
            organization_id = "b100145e-0000-0000-0000-000000000000"
            "#,
        )
        .expect("pre-KMS [wallet] config must still parse");

        assert!(settings.kms_api_key.is_none());
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
            TurnkeyOrganizationId::new("org-test".to_string()),
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

        let wallet = TurnkeyWallet::new(WalletCtx {
            settings: TurnkeySettings {
                address,
                organization_id: TurnkeyOrganizationId::new(org_id),
                kms_api_key: None,
            },
            credentials: TurnkeyCredentials {
                api_private_key: Some(TurnkeyApiPrivateKey::new(api_key)),
            },
            provider,
            required_confirmations: 1,
        })
        .await
        .expect("failed to construct TurnkeyWallet from env vars");

        (wallet, anvil)
    }

    #[ignore = "requires TURNKEY_* env vars -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn turnkey_integration() {
        let (api_key, org_id, address) = turnkey_env()
            .expect("TURNKEY_API_PRIVATE_KEY, TURNKEY_ORG_ID, and TURNKEY_ADDRESS must be set");

        let (wallet, _anvil) = integration_wallet(api_key, org_id, address).await;
        let self_address = wallet.address();

        // Wallet address matches configured address.
        assert_eq!(wallet.address(), address);

        // Sequential 0-value self-transfer: exercises the full Turnkey
        // signing round-trip.
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
