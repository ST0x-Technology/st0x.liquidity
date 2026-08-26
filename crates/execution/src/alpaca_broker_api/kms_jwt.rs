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
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use super::auth::AlpacaBrokerAuth;
use crate::rate_limit::retry_after_from_response_headers;

/// Alpaca's token endpoint for live broker partners. Doubles as the
/// assertion audience, per RFC 7523.
pub const ALPACA_TOKEN_URL: &str = "https://authx.alpaca.markets/v1/oauth2/token";

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
    MalformedSignature(#[from] p256::ecdsa::Error),
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

/// Signs client assertions with Cloud KMS and exchanges them for
/// cached bearer tokens.
///
/// Shared behind an `Arc` so every clone of one client reuses that
/// client's token cache. (Each client builds its own runtime today, so
/// a process runs one cache per client; Alpaca accepts concurrent
/// tokens per client_id, observed live 2026-08-25.)
pub struct KmsJwtAuth {
    client_id: String,
    /// Full KMS key-version resource name
    /// (`projects/.../cryptoKeyVersions/1`).
    kms_key_version: String,
    token_url: String,
    kms_base_url: String,
    metadata_token_url: String,
    http: reqwest::Client,
    /// Read on every request (std mutex, never held across await); a
    /// caller holding a still-valid token never waits on a mint.
    cached: StdMutex<Option<CachedToken>>,
    /// Serializes mints so concurrent stale callers cannot stampede the
    /// token endpoint.
    mint_lock: Mutex<()>,
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
            .field("kms_key_version", &self.kms_key_version)
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
            kms_key_version: kms_key_version.to_string(),
            token_url: token_url.to_string(),
            kms_base_url: kms_base_url.to_string(),
            metadata_token_url: metadata_token_url.to_string(),
            http,
            cached: StdMutex::new(None),
            mint_lock: Mutex::new(()),
        }
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
            "Minted Alpaca access token via KMS client assertion"
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

        let digest = Sha256::digest(signing_input.as_bytes());
        let der = self.kms_sign(&digest).await?;
        let raw = der_ecdsa_to_raw(&der)?;
        Ok(format!("{signing_input}.{}", BASE64_URL.encode(raw)))
    }

    /// One KMS `AsymmetricSign` over a SHA-256 digest, authorized by the
    /// instance service account's metadata token. Returns the DER
    /// signature bytes.
    ///
    /// `GOOGLE_OAUTH_ACCESS_TOKEN` overrides the metadata server, the
    /// same break-glass the Turnkey stamper honors, so an operator can
    /// run the bot off-VM with `gcloud auth print-access-token`.
    async fn kms_sign(&self, digest: &[u8]) -> Result<Vec<u8>, KmsJwtError> {
        let access_token = match std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
            Ok(token) if !token.trim().is_empty() => token,
            _ => {
                self.http
                    .get(&self.metadata_token_url)
                    .header("Metadata-Flavor", "Google")
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<MetadataToken>()
                    .await?
                    .access_token
            }
        };

        let url = format!(
            "{}/{}:asymmetricSign",
            self.kms_base_url, self.kms_key_version
        );
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
    let sig = p256::ecdsa::Signature::from_der(der)?;
    Ok(sig.to_bytes().into())
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
    pub fn build(auth: AlpacaBrokerAuth) -> Result<Self, KmsJwtError> {
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
                // Own client: the mint's three round-trips (metadata,
                // KMS, token endpoint) should not inherit the broker
                // client's default headers.
                let http = reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(10))
                    .timeout(Duration::from_secs(10))
                    .build()?;
                Ok(Self::KmsJwt(std::sync::Arc::new(KmsJwtAuth::new(
                    &client_id,
                    &kms_key_version,
                    http,
                ))))
            }
        }
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

    fn basic_runtime() -> AuthRuntime {
        AuthRuntime::build(AlpacaBrokerAuth::Basic {
            api_key: "key-id".to_string(),
            api_secret: "secret".to_string(),
        })
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
        let expired = Instant::now().checked_sub(Duration::from_secs(1)).unwrap();
        let mut cached = auth.cached.lock().unwrap();
        let token = cached.as_mut().unwrap();
        token.refresh_after = expired;
        token.hard_expiry = expired;
        drop(cached);
        assert!(matches!(
            auth.access_token().await,
            Err(KmsJwtError::TokenStatus { status: 500, .. })
        ));
    }
}
