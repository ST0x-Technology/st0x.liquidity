//! Keyless Alpaca authentication: RFC 7523 client assertions signed by
//! a non-extractable Cloud KMS key.
//!
//! Alpaca's client-credentials flow accepts a `private_key_jwt` client
//! assertion: a short-lived ES256 JWT whose public half is registered on
//! a BrokerDash credential. This module holds the private half as an
//! `EC_SIGN_P256_SHA256` key in Cloud KMS and signs each assertion via
//! `AsymmetricSign`, authorized by the VM's ambient GCP identity (the
//! attached service account) instead of a stored secret; the assertion
//! buys a bearer token (valid 15 minutes) that authenticates Broker API
//! and Market Data API calls alike. Same posture as the Turnkey stamper
//! in `st0x-evm::gcp_kms_stamper`, and the same module shape as
//! st0x.pricing's `alpaca_auth` (which has run keyless-only in its
//! staging since 2026-08-25):
//!
//!   - the `[broker]` api_key/api_secret disappear from the secrets
//!     TOML: nothing to store, rotate, or exfiltrate;
//!   - IAM (`roles/cloudkms.signerVerifier` on the key) decides who can
//!     authenticate to Alpaca, and every signature lands in the KMS
//!     data-access audit log;
//!   - per-environment isolation is a per-env key + per-env BrokerDash
//!     credential, not a shared secret.
//!
//! The token cache degrades instead of stalling or failing: it refreshes
//! two minutes early, callers arriving during an in-flight refresh ride
//! the still-valid cached token rather than queueing behind three HTTP
//! round-trips, and a failed refresh falls back to the cached token
//! until shortly before its true expiry.

use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64_STD, URL_SAFE_NO_PAD as BASE64_URL};
use p256::{SecretKey, ecdsa, elliptic_curve, pkcs8};
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::auth::AlpacaBrokerAuth;
use crate::rate_limit::retry_after_from_response_headers;

/// Alpaca's token endpoint for live broker partners. Doubles as the
/// assertion audience, per RFC 7523.
pub const ALPACA_TOKEN_URL: &str = "https://authx.alpaca.markets/v1/oauth2/token";

/// The sandbox token endpoint: sandbox BrokerDash credentials mint here,
/// mirroring the broker/market-data host split by mode.
pub(crate) const ALPACA_SANDBOX_TOKEN_URL: &str =
    "https://authx.sandbox.alpaca.markets/v1/oauth2/token";

const DEFAULT_KMS_BASE_URL: &str = "https://cloudkms.googleapis.com/v1";
const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

/// Client assertions are single-use and short-lived; five minutes is the
/// RFC's suggested order of magnitude.
const ASSERTION_TTL: Duration = Duration::from_secs(300);

/// Refresh the cached access token this long before it expires, so a
/// request never rides a token that dies mid-flight.
const TOKEN_REFRESH_MARGIN: Duration = Duration::from_secs(120);

/// Stop USING a cached token this long before its stated expiry.
const TOKEN_HARD_MARGIN: Duration = Duration::from_secs(10);

/// After a failed refresh that fell back to the cached token, wait this
/// long before the next attempt: without it, sub-second pollers would
/// re-run the full three-round-trip mint back to back for the whole
/// refresh window.
const FAILED_MINT_BACKOFF: Duration = Duration::from_secs(15);

/// Connect and request timeout for each mint round-trip (KMS sign,
/// metadata token, authx exchange). A compile-time constant like the
/// broker client's `HTTP_REQUEST_TIMEOUT`, not deployment config: the
/// bound protects the token cache's refresh margin, and no environment
/// tunes it independently.
const MINT_HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Ceiling on the token lifetime we believe from the endpoint. Alpaca
/// says 15 minutes; a wild `expires_in` must not push the expiry
/// Instants past what the arithmetic below can represent.
const TOKEN_MAX_LIFETIME: Duration = Duration::from_secs(3600);

/// Errors from minting an Alpaca access token via Cloud KMS. HTTP error
/// bodies are Google/Alpaca error JSON; no tokens or signatures are ever
/// embedded.
#[derive(Debug, thiserror::Error)]
pub enum KmsJwtError {
    #[error("KMS JWT HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("KMS AsymmetricSign returned HTTP {status}: {body}")]
    KmsStatus {
        status: u16,
        body: String,
        /// KMS's `Retry-After` hint on a 429, kept for backpressure.
        retry_after: Option<Duration>,
    },
    #[error("Alpaca token endpoint returned HTTP {status}: {body}")]
    TokenStatus {
        status: u16,
        body: String,
        /// The endpoint's `Retry-After` hint, captured so a token 429
        /// keeps its backpressure signal through the error chain.
        retry_after: Option<Duration>,
    },
    #[error("base64 decode of KMS response failed: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("malformed DER ECDSA signature from KMS: {0}")]
    MalformedSignature(#[from] ecdsa::Error),
    /// Same source type as [`Self::MalformedSignature`] but a different
    /// operation: the local PEM key failed to produce a signature at
    /// all. Mapped explicitly (no `#[from]`) since the derive can only
    /// route `ecdsa::Error` to one variant.
    #[error("local PEM signing failed: {0}")]
    LocalSign(#[source] ecdsa::Error),
    #[error("invalid SEC1 EC private key PEM: {0}")]
    Sec1PrivateKey(#[from] elliptic_curve::Error),
    #[error("invalid PKCS#8 private key PEM: {0}")]
    Pkcs8PrivateKey(#[from] pkcs8::Error),
    #[error("claims serialization failed: {0}")]
    Claims(#[from] serde_json::Error),
    #[error("credential contains bytes invalid in an HTTP header: {0}")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),
    #[error("system clock is before the UNIX epoch")]
    ClockBeforeEpoch,
}

impl KmsJwtError {
    /// True when retrying the same mint deterministically fails again:
    /// a non-429/408 HTTP 4xx from KMS or the token endpoint (revoked
    /// IAM grant, disabled or mis-registered BrokerDash credential),
    /// or a local encoding failure. Under Basic auth the equivalent
    /// 401/403 classifies Permanent via `status_permanence`; this keeps
    /// keyless auth failing fast the same way.
    pub fn is_deterministic(&self) -> bool {
        match self {
            Self::KmsStatus { status, .. } | Self::TokenStatus { status, .. } => {
                (400..500).contains(status) && *status != 429 && *status != 408
            }
            Self::Base64(_)
            | Self::MalformedSignature(_)
            | Self::LocalSign(_)
            | Self::Sec1PrivateKey(_)
            | Self::Pkcs8PrivateKey(_)
            | Self::Claims(_)
            | Self::InvalidHeader(_)
            | Self::ClockBeforeEpoch => true,
            Self::Http(_) => false,
        }
    }

    /// True when the token endpoint rate-limited the mint.
    pub fn is_rate_limited(&self) -> bool {
        matches!(
            self,
            Self::TokenStatus { status: 429, .. } | Self::KmsStatus { status: 429, .. }
        )
    }

    /// The rate-limiting endpoint's `Retry-After` hint, when sent.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Self::TokenStatus { retry_after, .. } | Self::KmsStatus { retry_after, .. } => {
                *retry_after
            }
            _ => None,
        }
    }
}

/// Signs client assertions and exchanges them for cached bearer tokens.
///
/// The signature comes from one of two places (see [`AssertionSigner`]);
/// everything else -- the assertion claims, the token exchange, and the
/// cache -- is shared between them.
///
/// Shared behind an `Arc` so every clone of one client reuses that
/// client's token cache. (Each client builds its own runtime today, so
/// a process runs one cache per client; Alpaca accepts concurrent
/// tokens per client_id, observed live 2026-08-25.)
pub struct KmsJwtAuth {
    client_id: String,
    signer: AssertionSigner,
    token_url: String,
    http: reqwest::Client,
    /// Read on every request (std mutex, never held across await); a
    /// caller holding a still-valid token never waits on a mint.
    cached: StdMutex<Option<CachedToken>>,
    /// Serializes mints so concurrent stale callers cannot stampede the
    /// token endpoint.
    mint_lock: Mutex<()>,
}

/// Where the ES256 signature over a client assertion comes from.
enum AssertionSigner {
    /// A non-extractable `EC_SIGN_P256_SHA256` key in Cloud KMS,
    /// authorized by ambient GCP identity. The production posture.
    Kms {
        /// Full KMS key-version resource name
        /// (`projects/.../cryptoKeyVersions/1`).
        kms_key_version: String,
        kms_base_url: String,
        metadata_token_url: String,
    },
    /// The BrokerDash credential's EC P-256 private key held in memory
    /// (parsed from the dashboard's `private_key_jwt` PEM export). For
    /// operator/CLI use against the sandbox, where no KMS key or IAM
    /// grant exists; the production bot stays on [`Self::Kms`].
    LocalPem(ecdsa::SigningKey),
}

// The signing key must never reach logs.
impl std::fmt::Debug for AssertionSigner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kms {
                kms_key_version, ..
            } => f
                .debug_struct("Kms")
                .field("kms_key_version", kms_key_version)
                .finish_non_exhaustive(),
            Self::LocalPem(_) => f.debug_tuple("LocalPem").field(&"[REDACTED]").finish(),
        }
    }
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    /// Soft deadline: past this, try to mint a replacement.
    refresh_after: Instant,
    /// Hard deadline: past this, the token must not be sent.
    hard_expiry: Instant,
}

// Manual Debug impls: the cache holds a LIVE bearer token, which must
// never reach logs (the same reason the ctx and client Debug impls
// redact credentials).
impl std::fmt::Debug for KmsJwtAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsJwtAuth")
            .field("client_id", &self.client_id)
            .field("signer", &self.signer)
            .field("token_url", &self.token_url)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Debug for CachedToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachedToken")
            .field("access_token", &"[REDACTED]")
            .field("refresh_after", &self.refresh_after)
            .field("hard_expiry", &self.hard_expiry)
            .finish()
    }
}

#[derive(Debug, Deserialize)]
struct TokenResponse {
    access_token: String,
    expires_in: u64,
}

#[derive(Debug, Deserialize)]
struct MetadataToken {
    access_token: String,
}

#[derive(Debug, Deserialize)]
struct SignResponse {
    signature: String,
}

impl KmsJwtAuth {
    pub fn new(client_id: &str, kms_key_version: &str, http: reqwest::Client) -> Self {
        Self::with_urls(
            client_id,
            kms_key_version,
            http,
            ALPACA_TOKEN_URL,
            DEFAULT_KMS_BASE_URL,
            METADATA_TOKEN_URL,
        )
    }

    pub fn with_urls(
        client_id: &str,
        kms_key_version: &str,
        http: reqwest::Client,
        token_url: &str,
        kms_base_url: &str,
        metadata_token_url: &str,
    ) -> Self {
        Self {
            client_id: client_id.to_string(),
            signer: AssertionSigner::Kms {
                kms_key_version: kms_key_version.to_string(),
                kms_base_url: kms_base_url.to_string(),
                metadata_token_url: metadata_token_url.to_string(),
            },
            token_url: token_url.to_string(),
            http,
            cached: StdMutex::new(None),
            mint_lock: Mutex::new(()),
        }
    }

    /// Signs assertions with a locally held EC P-256 private key (the
    /// BrokerDash `private_key_jwt` PEM export). Accepts SEC1
    /// (`BEGIN EC PRIVATE KEY`) and PKCS#8 (`BEGIN PRIVATE KEY`) PEMs.
    ///
    /// The raw export file works as-is: exports bundle the key with
    /// other PEM blocks (`EC PARAMETERS`, the public key), and only the
    /// private-key block between its BEGIN/END markers is parsed.
    fn local_pem(
        client_id: &str,
        private_key_pem: &str,
        http: reqwest::Client,
        token_url: &str,
    ) -> Result<Self, KmsJwtError> {
        const SEC1_BEGIN: &str = "-----BEGIN EC PRIVATE KEY-----";
        const SEC1_END: &str = "-----END EC PRIVATE KEY-----";
        const PKCS8_BEGIN: &str = "-----BEGIN PRIVATE KEY-----";
        const PKCS8_END: &str = "-----END PRIVATE KEY-----";

        let secret_key = if let Some(wrapped_body) =
            rewrapped_block_body(private_key_pem, SEC1_BEGIN, SEC1_END)
        {
            let normalized = format!("{SEC1_BEGIN}\n{wrapped_body}\n{SEC1_END}\n");
            match SecretKey::from_sec1_pem(&normalized) {
                Ok(secret_key) => secret_key,
                Err(sec1_error) => {
                    // Some exports wrap a PKCS#8 document in SEC1
                    // markers (openssl parses by content, not label), so
                    // retry the same body as PKCS#8. When both parses
                    // fail, report the error matching the block's
                    // declared SEC1 label.
                    let relabeled = format!("{PKCS8_BEGIN}\n{wrapped_body}\n{PKCS8_END}\n");
                    pkcs8::DecodePrivateKey::from_pkcs8_pem(&relabeled)
                        .map_err(|_: pkcs8::Error| KmsJwtError::Sec1PrivateKey(sec1_error))?
                }
            }
        } else if let Some(wrapped_body) =
            rewrapped_block_body(private_key_pem, PKCS8_BEGIN, PKCS8_END)
        {
            let normalized = format!("{PKCS8_BEGIN}\n{wrapped_body}\n{PKCS8_END}\n");
            pkcs8::DecodePrivateKey::from_pkcs8_pem(&normalized)?
        } else {
            // No recognizable private-key markers: hand the input to the
            // PKCS#8 parser so it reports the malformed PEM itself.
            pkcs8::DecodePrivateKey::from_pkcs8_pem(private_key_pem)?
        };

        Ok(Self {
            client_id: client_id.to_string(),
            signer: AssertionSigner::LocalPem(ecdsa::SigningKey::from(secret_key)),
            token_url: token_url.to_string(),
            http,
            cached: StdMutex::new(None),
            mint_lock: Mutex::new(()),
        })
    }

    /// The cached token if `deadline(token)` is still ahead of now.
    fn cached_before(&self, deadline: impl Fn(&CachedToken) -> Instant) -> Option<String> {
        // A panic while holding this lock cannot corrupt the value (it
        // only ever holds a fully-formed token), so recover from poison
        // instead of cascading the panic.
        let cached = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        cached
            .as_ref()
            .filter(|tok| Instant::now() < deadline(tok))
            .map(|tok| tok.access_token.clone())
    }

    /// The current access token, minting a fresh one when the cache is
    /// empty or past the soft refresh deadline.
    pub async fn access_token(&self) -> Result<String, KmsJwtError> {
        if let Some(token) = self.cached_before(|tok| tok.refresh_after) {
            return Ok(token);
        }

        let _minting = if let Ok(guard) = self.mint_lock.try_lock() {
            guard
        } else {
            // A mint is in flight. Only wait for it if the cached token
            // is unusable.
            if let Some(token) = self.cached_before(|tok| tok.hard_expiry) {
                return Ok(token);
            }
            self.mint_lock.lock().await
        };
        // Whoever held the lock before us may have refreshed already.
        if let Some(token) = self.cached_before(|tok| tok.refresh_after) {
            return Ok(token);
        }

        match self.mint().await {
            Ok(token) => Ok(token),
            Err(error) => {
                if let Some(token) = self.cached_before(|tok| tok.hard_expiry) {
                    tracing::warn!(
                        %error,
                        "Alpaca token refresh failed; riding the still-valid cached token"
                    );
                    self.defer_next_refresh();
                    return Ok(token);
                }
                Err(error)
            }
        }
    }

    /// Push the soft refresh deadline forward after a failed mint, so
    /// the next attempt waits [`FAILED_MINT_BACKOFF`] instead of firing
    /// on the very next request (still clamped to the hard expiry).
    fn defer_next_refresh(&self) {
        let mut cached = self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(token) = cached.as_mut() {
            token.refresh_after = (Instant::now() + FAILED_MINT_BACKOFF).min(token.hard_expiry);
        }
    }

    /// One sign + token-exchange round trip; on success the cache holds
    /// the new token and it is returned.
    async fn mint(&self) -> Result<String, KmsJwtError> {
        let assertion = self.sign_assertion().await?;
        let resp = self
            .http
            .post(&self.token_url)
            .form(&[
                ("grant_type", "client_credentials"),
                ("client_id", self.client_id.as_str()),
                (
                    "client_assertion_type",
                    "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                ),
                ("client_assertion", assertion.as_str()),
            ])
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let retry_after = retry_after_from_response_headers(resp.headers());
            return Err(KmsJwtError::TokenStatus {
                status: status.as_u16(),
                body: truncate_error_body(resp.text().await.unwrap_or_default()),
                retry_after,
            });
        }
        let token: TokenResponse = resp.json().await?;

        // Believe expires_in only up to a ceiling (an absurd value must
        // not overflow the Instant math), and keep a usable refresh
        // window even on short-lived tokens.
        let lifetime = Duration::from_secs(token.expires_in).min(TOKEN_MAX_LIFETIME);
        let now = Instant::now();
        let hard_expiry = now + lifetime.saturating_sub(TOKEN_HARD_MARGIN);
        // Clamped to the hard deadline: for a very short lifetime the
        // margin arithmetic could otherwise place the refresh AFTER the
        // point the token must no longer be sent.
        let refresh_after = (now
            + lifetime
                .saturating_sub(TOKEN_REFRESH_MARGIN)
                .max(lifetime / 2))
        .min(hard_expiry);
        tracing::info!(
            expires_in = token.expires_in,
            signer = match &self.signer {
                AssertionSigner::Kms { .. } => "kms",
                AssertionSigner::LocalPem(_) => "local-pem",
            },
            "Minted Alpaca access token via client assertion"
        );
        let access = token.access_token.clone();
        *self
            .cached
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(CachedToken {
            access_token: token.access_token,
            refresh_after,
            hard_expiry,
        });
        Ok(access)
    }

    /// Build and sign the ES256 client assertion for one token request.
    async fn sign_assertion(&self) -> Result<String, KmsJwtError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| KmsJwtError::ClockBeforeEpoch)?;

        // jti only has to be unique per assertion; a nanosecond stamp
        // plus a process-local counter is plenty without a rand dep.
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let jti = format!(
            "{}-{}-{}",
            now.as_nanos(),
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        );

        let header = BASE64_URL.encode(br#"{"alg":"ES256","typ":"JWT"}"#);
        let claims = BASE64_URL.encode(serde_json::to_vec(&serde_json::json!({
            "iss": self.client_id,
            "sub": self.client_id,
            "aud": self.token_url,
            "jti": jti,
            "iat": now.as_secs(),
            "exp": now.as_secs() + ASSERTION_TTL.as_secs(),
        }))?);
        let signing_input = format!("{header}.{claims}");

        let raw: [u8; 64] = match &self.signer {
            AssertionSigner::Kms {
                kms_key_version,
                kms_base_url,
                metadata_token_url,
            } => {
                let digest = Sha256::digest(signing_input.as_bytes());
                let der = self
                    .kms_sign(&digest, kms_key_version, kms_base_url, metadata_token_url)
                    .await?;
                der_ecdsa_to_raw(&der)?
            }
            // ES256 is ECDSA over SHA-256 of the message; the signer
            // hashes internally and yields the raw `r || s` directly.
            // `try_sign`, not `sign`: the infallible wrapper panics on a
            // signing error, which production code must never do.
            AssertionSigner::LocalPem(key) => {
                let signature: ecdsa::Signature =
                    ecdsa::signature::Signer::try_sign(key, signing_input.as_bytes())
                        .map_err(KmsJwtError::LocalSign)?;
                signature.to_bytes().into()
            }
        };
        Ok(format!("{signing_input}.{}", BASE64_URL.encode(raw)))
    }

    /// One KMS `AsymmetricSign` over a SHA-256 digest, authorized by the
    /// instance service account's metadata token. Returns the DER
    /// signature bytes.
    ///
    /// `GOOGLE_OAUTH_ACCESS_TOKEN` overrides the metadata server, the
    /// same break-glass the Turnkey stamper honors, so an operator can
    /// run the bot off-VM with `gcloud auth print-access-token`.
    async fn kms_sign(
        &self,
        digest: &[u8],
        kms_key_version: &str,
        kms_base_url: &str,
        metadata_token_url: &str,
    ) -> Result<Vec<u8>, KmsJwtError> {
        let access_token = match std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
            Ok(token) if !token.trim().is_empty() => token,
            _ => {
                self.http
                    .get(metadata_token_url)
                    .header("Metadata-Flavor", "Google")
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<MetadataToken>()
                    .await?
                    .access_token
            }
        };

        let url = format!("{kms_base_url}/{kms_key_version}:asymmetricSign");
        let resp = self
            .http
            .post(&url)
            .bearer_auth(access_token)
            .json(&serde_json::json!({
                "digest": { "sha256": BASE64_STD.encode(digest) }
            }))
            .send()
            .await?;
        let status = resp.status();
        if !status.is_success() {
            let retry_after = retry_after_from_response_headers(resp.headers());
            return Err(KmsJwtError::KmsStatus {
                status: status.as_u16(),
                body: truncate_error_body(resp.text().await.unwrap_or_default()),
                retry_after,
            });
        }
        let signed: SignResponse = resp.json().await?;
        Ok(BASE64_STD.decode(signed.signature)?)
    }
}

/// Error bodies are Google/Alpaca error JSON, but nothing guarantees an
/// endpoint never echoes request material back; cap what an error can
/// carry into logs.
fn truncate_error_body(mut body: String) -> String {
    const MAX: usize = 512;
    if body.len() > MAX {
        let mut end = MAX;
        while !body.is_char_boundary(end) {
            end -= 1;
        }
        body.truncate(end);
        body.push_str("... [truncated]");
    }
    body
}

/// KMS returns ECDSA signatures DER-encoded (`SEQUENCE { INTEGER r,
/// INTEGER s }`); JOSE ES256 wants the raw 64-byte `r || s`. p256 does
/// the parsing (and rejects out-of-range scalars), the same crate the
/// Turnkey stamper leans on.
fn der_ecdsa_to_raw(der: &[u8]) -> Result<[u8; 64], KmsJwtError> {
    let sig = ecdsa::Signature::from_der(der)?;
    Ok(sig.to_bytes().into())
}

/// The base64 body of the PEM block delimited by the given markers,
/// whitespace-stripped and re-wrapped at 64 columns: the strict RFC 7468
/// parser rejects longer lines, and exports write the body as one long
/// line (openssl tolerates that, the parser does not). Bounding at the
/// end marker keeps blocks before or after the key (parameters, the
/// public key) away from the parser. `None` when the begin marker is
/// absent; a missing end marker leaves trailing junk in the body, which
/// the key parser then reports itself.
fn rewrapped_block_body(pem: &str, begin_marker: &str, end_marker: &str) -> Option<String> {
    let block_start = pem.find(begin_marker)?;
    let after_begin = &pem[block_start + begin_marker.len()..];
    let body = after_begin
        .find(end_marker)
        .map_or(after_begin, |end_offset| &after_begin[..end_offset]);

    let mut wrapped_body = String::with_capacity(body.len() + body.len() / 64 + 1);
    for (index, character) in body.split_whitespace().flat_map(str::chars).enumerate() {
        if index > 0 && index % 64 == 0 {
            wrapped_body.push('\n');
        }
        wrapped_body.push(character);
    }
    Some(wrapped_body)
}

/// Runtime side of [`AlpacaBrokerAuth`]: precomputed header values for
/// the Basic pair, a shared token cache for keyless.
#[derive(Clone)]
pub enum AuthRuntime {
    Basic {
        /// `Basic <base64(key:secret)>` for the Broker API.
        authorization: HeaderValue,
        /// The raw pair for the Market Data API's APCA headers.
        api_key: HeaderValue,
        api_secret: HeaderValue,
    },
    KmsJwt(std::sync::Arc<KmsJwtAuth>),
}

impl AuthRuntime {
    /// Builds the runtime for `auth`, minting JWT-variant tokens at
    /// `token_url` (derive it from the mode so sandbox credentials mint
    /// at the sandbox authx host; Basic ignores it).
    pub fn build(auth: AlpacaBrokerAuth, token_url: &str) -> Result<Self, KmsJwtError> {
        match auth {
            AlpacaBrokerAuth::Basic {
                api_key,
                api_secret,
            } => {
                let credentials = format!("{api_key}:{api_secret}");
                let encoded = BASE64_STD.encode(credentials.as_bytes());
                let mut authorization = HeaderValue::from_str(&format!("Basic {encoded}"))?;
                authorization.set_sensitive(true);
                let mut api_key = HeaderValue::from_str(&api_key)?;
                api_key.set_sensitive(true);
                let mut api_secret = HeaderValue::from_str(&api_secret)?;
                api_secret.set_sensitive(true);
                Ok(Self::Basic {
                    authorization,
                    api_key,
                    api_secret,
                })
            }
            AlpacaBrokerAuth::KmsJwt {
                client_id,
                kms_key_version,
            } => {
                let http = Self::mint_http_client()?;
                Ok(Self::KmsJwt(std::sync::Arc::new(KmsJwtAuth::with_urls(
                    &client_id,
                    &kms_key_version,
                    http,
                    token_url,
                    DEFAULT_KMS_BASE_URL,
                    METADATA_TOKEN_URL,
                ))))
            }
            AlpacaBrokerAuth::PrivateKeyJwt {
                client_id,
                private_key_pem,
            } => {
                let http = Self::mint_http_client()?;
                Ok(Self::KmsJwt(std::sync::Arc::new(KmsJwtAuth::local_pem(
                    &client_id,
                    &private_key_pem,
                    http,
                    token_url,
                )?)))
            }
        }
    }

    /// Own client for the mint round-trips: they should not inherit the
    /// broker client's default headers.
    fn mint_http_client() -> Result<reqwest::Client, KmsJwtError> {
        Ok(reqwest::Client::builder()
            .connect_timeout(MINT_HTTP_TIMEOUT)
            .timeout(MINT_HTTP_TIMEOUT)
            .build()?)
    }

    /// `Authorization` header for a Broker API request.
    pub async fn broker_authorization(&self) -> Result<HeaderValue, KmsJwtError> {
        match self {
            Self::Basic { authorization, .. } => Ok(authorization.clone()),
            Self::KmsJwt(auth) => {
                let token = auth.access_token().await?;
                let mut value = HeaderValue::from_str(&format!("Bearer {token}"))?;
                value.set_sensitive(true);
                Ok(value)
            }
        }
    }

    /// Attach APCA-style credentials to a request: the APCA header pair
    /// for Basic, the same bearer token for keyless (verified accepted
    /// by data.alpaca.markets and the tokenization endpoints,
    /// 2026-08-25). Used by the Market Data API and the tokenization
    /// client alike.
    pub async fn apply_apca(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, KmsJwtError> {
        match self {
            Self::Basic {
                api_key,
                api_secret,
                ..
            } => Ok(request
                .header(HeaderName::from_static("apca-api-key-id"), api_key.clone())
                .header(
                    HeaderName::from_static("apca-api-secret-key"),
                    api_secret.clone(),
                )),
            Self::KmsJwt(_) => {
                Ok(request.header(AUTHORIZATION, self.broker_authorization().await?))
            }
        }
    }

    /// Attach Alpaca wallet-endpoint credentials to a request: the
    /// APCA treatment plus, for the legacy pair, the Basic
    /// `Authorization` the wallet endpoints historically wanted on top.
    pub async fn apply_wallet(
        &self,
        request: reqwest::RequestBuilder,
    ) -> Result<reqwest::RequestBuilder, KmsJwtError> {
        let request = self.apply_apca(request).await?;
        Ok(match self {
            Self::Basic { authorization, .. } => {
                request.header(AUTHORIZATION, authorization.clone())
            }
            Self::KmsJwt(_) => request,
        })
    }
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;

    use super::*;

    // SEQUENCE of two INTEGERs, DER-style.
    fn der(r_int: &[u8], s_int: &[u8]) -> Vec<u8> {
        let total = u8::try_from(4 + r_int.len() + s_int.len()).unwrap();
        let mut out = vec![0x30, total];
        for int in [r_int, s_int] {
            out.push(0x02);
            out.push(u8::try_from(int.len()).unwrap());
            out.extend_from_slice(int);
        }
        out
    }

    #[test]
    fn der_full_width_integers_pass_through() {
        let r_int = [0x11u8; 32];
        let s_int = [0x22u8; 32];
        let raw = der_ecdsa_to_raw(&der(&r_int, &s_int)).unwrap();
        assert_eq!(&raw[..32], &r_int);
        assert_eq!(&raw[32..], &s_int);
    }

    #[test]
    fn der_short_integers_left_pad_and_leading_zero_strips() {
        // r fits in 31 bytes; s carries the 0x00 prefix DER adds when
        // the high bit is set (0xF0... stays below the group order).
        let r_int = [0x01u8; 31];
        let mut s_int = vec![0x00];
        s_int.extend_from_slice(&[0xF0u8; 32]);
        let raw = der_ecdsa_to_raw(&der(&r_int, &s_int)).unwrap();
        assert_eq!(raw[0], 0x00);
        assert_eq!(&raw[1..32], &r_int);
        assert_eq!(&raw[32..], &[0xF0u8; 32]);
    }

    #[test]
    fn der_garbage_is_rejected() {
        assert!(matches!(
            der_ecdsa_to_raw(&[]),
            Err(KmsJwtError::MalformedSignature(_))
        ));
        assert!(matches!(
            der_ecdsa_to_raw(&[0x30, 0x02, 0x02, 0x00]),
            Err(KmsJwtError::MalformedSignature(_))
        ));
        let mut trailing = der(&[0x01; 32], &[0x02; 32]);
        trailing[1] -= 1; // length no longer covers the whole buffer
        assert!(matches!(
            der_ecdsa_to_raw(&trailing),
            Err(KmsJwtError::MalformedSignature(_))
        ));
    }

    fn stub_auth(server: &MockServer) -> KmsJwtAuth {
        KmsJwtAuth::with_urls(
            "CKTEST",
            "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1",
            reqwest::Client::new(),
            &server.url("/token"),
            &server.base_url(),
            &server.url("/meta"),
        )
    }

    fn mock_sign_chain(server: &MockServer) {
        server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/meta");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "metadata-token" }));
        });
        server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path_includes(":asymmetricSign");
            then.status(200).json_body(serde_json::json!({
                "signature": BASE64_STD.encode(der(&[0x11; 32], &[0x22; 32]))
            }));
        });
    }

    #[tokio::test]
    async fn mints_a_well_formed_assertion_and_caches_the_token() {
        let server = MockServer::start_async().await;
        mock_sign_chain(&server);
        let token_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/token")
                .body_includes("grant_type=client_credentials")
                .body_includes("client_id=CKTEST")
                .body_includes(
                    "client_assertion_type=urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer",
                );
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok-1", "expires_in": 900 }));
        });

        let auth = stub_auth(&server);
        assert_eq!(auth.access_token().await.unwrap(), "tok-1");
        // Second call inside the refresh window: cached, no new exchange.
        assert_eq!(auth.access_token().await.unwrap(), "tok-1");
        token_mock.assert_calls(1);
    }

    /// A deterministic P-256 key for the local-PEM tests, round-tripped
    /// through the same SEC1 PEM encoding the BrokerDash export uses.
    fn test_key_pem() -> (SecretKey, String) {
        let secret = SecretKey::from_slice(&[0x37; 32]).unwrap();
        let pem = secret
            .to_sec1_pem(pkcs8::LineEnding::LF)
            .unwrap()
            .to_string();
        (secret, pem)
    }

    #[tokio::test]
    async fn local_pem_assertion_verifies_against_the_public_key() {
        let (secret, pem) = test_key_pem();
        let auth = KmsJwtAuth::local_pem(
            "CKLOCAL",
            &pem,
            reqwest::Client::new(),
            "https://authx.test/t",
        )
        .unwrap();

        let assertion = auth.sign_assertion().await.unwrap();
        let [header, claims, signature]: [&str; 3] =
            assertion.split('.').collect::<Vec<_>>().try_into().unwrap();

        let claims: serde_json::Value =
            serde_json::from_slice(&BASE64_URL.decode(claims).unwrap()).unwrap();
        assert_eq!(claims["iss"], "CKLOCAL");
        assert_eq!(claims["sub"], "CKLOCAL");
        assert_eq!(claims["aud"], "https://authx.test/t");

        assert_eq!(
            String::from_utf8(BASE64_URL.decode(header).unwrap()).unwrap(),
            r#"{"alg":"ES256","typ":"JWT"}"#
        );

        let verifying = ecdsa::VerifyingKey::from(secret.public_key());
        let raw = BASE64_URL.decode(signature).unwrap();
        let signature = ecdsa::Signature::from_slice(&raw).unwrap();
        let signing_input = assertion.rsplit_once('.').unwrap().0;
        ecdsa::signature::Verifier::verify(&verifying, signing_input.as_bytes(), &signature)
            .expect("the local ES256 signature must verify against the key's public half");
    }

    #[tokio::test]
    async fn local_pem_mints_without_any_kms_round_trip() {
        // Only the token endpoint is mocked: a metadata or KMS request
        // would hit an unmatched route and fail the mint, so success
        // proves the local signer makes no KMS round trips.
        let server = MockServer::start_async().await;
        let token_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/token")
                .body_includes("grant_type=client_credentials")
                .body_includes("client_id=CKLOCAL")
                .body_includes("client_assertion=");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok-local", "expires_in": 900 }));
        });

        let (_, pem) = test_key_pem();
        let auth = KmsJwtAuth::local_pem(
            "CKLOCAL",
            &pem,
            reqwest::Client::new(),
            &server.url("/token"),
        )
        .unwrap();

        assert_eq!(auth.access_token().await.unwrap(), "tok-local");
        // Cached inside the refresh window: still exactly one exchange.
        assert_eq!(auth.access_token().await.unwrap(), "tok-local");
        token_mock.assert_calls(1);
    }

    #[tokio::test]
    async fn private_key_jwt_runtime_builds_and_mints_through_the_token_url() {
        // The AuthRuntime::build branch, not the KmsJwtAuth constructor
        // directly: proves the config-level PrivateKeyJwt variant parses
        // the PEM, forwards the caller's token URL, and answers broker
        // requests with the minted bearer.
        let server = MockServer::start_async().await;
        let token_mock = server.mock(|when, then| {
            when.method(httpmock::Method::POST)
                .path("/token")
                .body_includes("grant_type=client_credentials")
                .body_includes("client_id=CKRUNTIME")
                .body_includes("client_assertion=");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok-runtime", "expires_in": 900 }));
        });

        let (_, pem) = test_key_pem();
        let runtime = AuthRuntime::build(
            AlpacaBrokerAuth::PrivateKeyJwt {
                client_id: "CKRUNTIME".to_string(),
                private_key_pem: pem,
            },
            &server.url("/token"),
        )
        .unwrap();

        let header = runtime.broker_authorization().await.unwrap();

        assert_eq!(header.to_str().unwrap(), "Bearer tok-runtime");
        token_mock.assert_calls(1);
    }

    #[tokio::test]
    async fn local_pem_accepts_an_ec_parameters_prefixed_export() {
        // Raw exports bundle the private key with other PEM blocks: the
        // BrokerDash export puts the public key first, openssl prepends
        // the named-curve parameters block (this base64 is the
        // prime256v1 OID), and trailing content can follow. Exports also
        // write the key body as ONE long base64 line, which strict
        // RFC 7468 parsing rejects unwrapped. The private-key block must
        // parse and sign from that bundle as-is.
        let (secret, pem) = test_key_pem();
        let single_line_body: String = pem
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        let bundled = format!(
            "-----BEGIN EC PUBLIC KEY-----\nfixture\n-----END EC PUBLIC KEY-----\n\n\
             -----BEGIN EC PARAMETERS-----\nBggqhkjOPQMBBw==\n-----END EC PARAMETERS-----\n\
             -----BEGIN EC PRIVATE KEY-----\n{single_line_body}\n\
             -----END EC PRIVATE KEY-----\ntrailing notes"
        );

        let auth = KmsJwtAuth::local_pem(
            "CKLOCAL",
            &bundled,
            reqwest::Client::new(),
            "https://authx.test/t",
        )
        .unwrap();

        let assertion = auth.sign_assertion().await.unwrap();
        let signing_input = assertion.rsplit_once('.').unwrap().0;
        let raw = BASE64_URL
            .decode(assertion.rsplit_once('.').unwrap().1)
            .unwrap();
        let signature = ecdsa::Signature::from_slice(&raw).unwrap();
        let verifying = ecdsa::VerifyingKey::from(secret.public_key());
        ecdsa::signature::Verifier::verify(&verifying, signing_input.as_bytes(), &signature)
            .expect("the bundled-PEM key must sign a verifiable assertion");
    }

    #[tokio::test]
    async fn local_pem_accepts_a_pkcs8_body_under_sec1_markers() {
        // The BrokerDash export observed 2026-08-27 wraps a PKCS#8
        // document in `EC PRIVATE KEY` markers, with the public key
        // block first and the body on one long line. openssl parses it
        // by content; local_pem must too.
        let (secret, _) = test_key_pem();
        let pkcs8_pem =
            pkcs8::EncodePrivateKey::to_pkcs8_pem(&secret, pkcs8::LineEnding::LF).unwrap();
        let single_line_body: String = pkcs8_pem
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        let bundled = format!(
            "-----BEGIN EC PUBLIC KEY-----\nfixture\n-----END EC PUBLIC KEY-----\n\n\
             -----BEGIN EC PRIVATE KEY-----\n{single_line_body}\n-----END EC PRIVATE KEY-----\n"
        );

        let auth = KmsJwtAuth::local_pem(
            "CKLOCAL",
            &bundled,
            reqwest::Client::new(),
            "https://authx.test/t",
        )
        .unwrap();

        let assertion = auth.sign_assertion().await.unwrap();
        let signing_input = assertion.rsplit_once('.').unwrap().0;
        let raw = BASE64_URL
            .decode(assertion.rsplit_once('.').unwrap().1)
            .unwrap();
        let signature = ecdsa::Signature::from_slice(&raw).unwrap();
        let verifying = ecdsa::VerifyingKey::from(secret.public_key());
        ecdsa::signature::Verifier::verify(&verifying, signing_input.as_bytes(), &signature)
            .expect("the PKCS#8-under-SEC1-markers key must sign a verifiable assertion");
    }

    #[tokio::test]
    async fn local_pem_accepts_a_single_line_pkcs8_export() {
        // A PKCS#8 export under its own markers gets the same 64-column
        // body normalization as the SEC1 branch: a single-line body must
        // parse and sign, not trip the strict parser's line limit.
        let (secret, _) = test_key_pem();
        let pkcs8_pem =
            pkcs8::EncodePrivateKey::to_pkcs8_pem(&secret, pkcs8::LineEnding::LF).unwrap();
        let single_line_body: String = pkcs8_pem
            .lines()
            .filter(|line| !line.starts_with("-----"))
            .collect();
        let flattened =
            format!("-----BEGIN PRIVATE KEY-----\n{single_line_body}\n-----END PRIVATE KEY-----\n");

        let auth = KmsJwtAuth::local_pem(
            "CKLOCAL",
            &flattened,
            reqwest::Client::new(),
            "https://authx.test/t",
        )
        .unwrap();

        let assertion = auth.sign_assertion().await.unwrap();
        let signing_input = assertion.rsplit_once('.').unwrap().0;
        let raw = BASE64_URL
            .decode(assertion.rsplit_once('.').unwrap().1)
            .unwrap();
        let signature = ecdsa::Signature::from_slice(&raw).unwrap();
        let verifying = ecdsa::VerifyingKey::from(secret.public_key());
        ecdsa::signature::Verifier::verify(&verifying, signing_input.as_bytes(), &signature)
            .expect("the single-line PKCS#8 key must sign a verifiable assertion");
    }

    #[test]
    fn local_pem_rejects_invalid_keys() {
        let sec1_garbage = "-----BEGIN EC PRIVATE KEY-----\nAAAA\n-----END EC PRIVATE KEY-----\n";
        assert!(matches!(
            KmsJwtAuth::local_pem("CK", sec1_garbage, reqwest::Client::new(), "https://t"),
            Err(KmsJwtError::Sec1PrivateKey(_))
        ));

        assert!(matches!(
            KmsJwtAuth::local_pem(
                "CK",
                "not a pem at all",
                reqwest::Client::new(),
                "https://t"
            ),
            Err(KmsJwtError::Pkcs8PrivateKey(_))
        ));
    }

    /// Expire both cache deadlines. Take-modify-reinsert keeps each
    /// `MutexGuard` a same-statement temporary, so neither the
    /// guard-across-await nor the significant-drop lint can fire.
    fn expire_cached_token(auth: &KmsJwtAuth) {
        let expired = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let mut token = auth.cached.lock().unwrap().take().unwrap();
        token.refresh_after = expired;
        token.hard_expiry = expired;
        *auth.cached.lock().unwrap() = Some(token);
    }

    fn basic_runtime() -> AuthRuntime {
        // Basic ignores the token URL; any value satisfies the signature.
        AuthRuntime::build(
            AlpacaBrokerAuth::Basic {
                api_key: "key-id".to_string(),
                api_secret: "secret".to_string(),
            },
            ALPACA_TOKEN_URL,
        )
        .unwrap()
    }

    fn built_headers(request: reqwest::RequestBuilder) -> reqwest::header::HeaderMap {
        request.build().unwrap().headers().clone()
    }

    #[tokio::test]
    async fn basic_apca_sends_the_header_pair_and_no_authorization() {
        let runtime = basic_runtime();
        let request = reqwest::Client::new().get("http://example.invalid");
        let headers = built_headers(runtime.apply_apca(request).await.unwrap());
        assert_eq!(headers.get("apca-api-key-id").unwrap(), "key-id");
        assert_eq!(headers.get("apca-api-secret-key").unwrap(), "secret");
        assert!(headers.get(AUTHORIZATION).is_none());
    }

    #[tokio::test]
    async fn basic_wallet_sends_basic_authorization_plus_the_apca_pair() {
        let runtime = basic_runtime();
        let request = reqwest::Client::new().get("http://example.invalid");
        let headers = built_headers(runtime.apply_wallet(request).await.unwrap());
        assert_eq!(headers.get("apca-api-key-id").unwrap(), "key-id");
        assert_eq!(headers.get("apca-api-secret-key").unwrap(), "secret");
        // base64("key-id:secret")
        assert_eq!(
            headers.get(AUTHORIZATION).unwrap(),
            "Basic a2V5LWlkOnNlY3JldA=="
        );
    }

    #[tokio::test]
    async fn kms_apca_and_wallet_send_the_bearer_token_only() {
        let server = MockServer::start_async().await;
        mock_sign_chain(&server);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/token");
            then.status(200)
                .json_body(serde_json::json!({ "access_token": "tok-1", "expires_in": 900 }));
        });
        let runtime = AuthRuntime::KmsJwt(std::sync::Arc::new(stub_auth(&server)));

        for apply_wallet in [false, true] {
            let request = reqwest::Client::new().get("http://example.invalid");
            let request = if apply_wallet {
                runtime.apply_wallet(request).await.unwrap()
            } else {
                runtime.apply_apca(request).await.unwrap()
            };
            let headers = built_headers(request);
            assert_eq!(headers.get(AUTHORIZATION).unwrap(), "Bearer tok-1");
            assert!(headers.get("apca-api-key-id").is_none());
            assert!(headers.get("apca-api-secret-key").is_none());
        }
    }

    #[tokio::test]
    async fn refresh_failure_rides_the_still_valid_cached_token() {
        let server = MockServer::start_async().await;
        mock_sign_chain(&server);
        server.mock(|when, then| {
            when.method(httpmock::Method::POST).path("/token");
            then.status(500).body("boom");
        });

        let auth = stub_auth(&server);
        // Seed a token past its soft refresh deadline but inside the
        // hard expiry: a failing refresh must fall back to it.
        *auth.cached.lock().unwrap() = Some(CachedToken {
            access_token: "seeded".into(),
            refresh_after: Instant::now().checked_sub(Duration::from_secs(1)).unwrap(),
            hard_expiry: Instant::now() + Duration::from_secs(60),
        });
        assert_eq!(auth.access_token().await.unwrap(), "seeded");

        // Past the hard expiry the failure must surface instead. (The
        // successful fallback above deferred refresh_after by the failed-
        // mint backoff, so expire both deadlines to model a cache whose
        // token is truly dead.)
        expire_cached_token(&auth);
        assert!(matches!(
            auth.access_token().await,
            Err(KmsJwtError::TokenStatus { status: 500, .. })
        ));
    }
}
