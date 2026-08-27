//! Verifying the IAP assertion on the role-gated ops API paths.
//!
//! Google's Identity-Aware Proxy authenticates the caller and stamps every
//! request it forwards with `x-goog-iap-jwt-assertion`, an ES256 JWT signed by
//! Google. The load balancer in front of this bot routes each role prefix to
//! its own backend service, each with its own IAP policy bound to a Workspace
//! group, so IAP has already decided whether this identity may use this
//! prefix by the time a request arrives.
//!
//! This module is the second gate, and it exists because the first one is not
//! reachable from everywhere. Anything already inside the VPC can talk to the
//! VM without passing the load balancer at all, and in that path there is no
//! IAP and no group check. Verifying the assertion here means the app itself
//! refuses a request that did not come through IAP, rather than trusting the
//! network.
//!
//! The audience is the load-bearing claim. IAP binds the token it mints to the
//! backend service that admitted the caller, so a token issued for the read
//! backend carries the read backend's audience. Pinning the expected audience
//! per prefix therefore turns a replay of a read-tier token against the write
//! path into a rejection, even if the URL map were misconfigured to route it
//! there. Checking only the signature would miss exactly that case.
//!
//! What this module deliberately does NOT do is decide who may do what. That
//! is group membership, evaluated by IAP against the backend's IAM policy.
//! Duplicating it here as an email allowlist would create a second source of
//! truth that drifts, and would defeat the point of the pilot, which is that
//! granting an operator write access is a Workspace admin console change.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::Request;
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use serde::Deserialize;
use tokio::sync::RwLock;
use tracing::{info, warn};

/// Header IAP stamps on every request it forwards.
const ASSERTION_HEADER: &str = "x-goog-iap-jwt-assertion";

/// IAP's signing keys. A distinct endpoint from Google's other JWKS documents:
/// IAP assertions are signed with ES256 keys published only here.
const IAP_JWKS_URL: &str = "https://www.gstatic.com/iap/verify/public_key-jwk";

/// Only IAP mints these.
const IAP_ISSUER: &str = "https://cloud.google.com/iap";

/// How long a fetched key set is served before a refresh is attempted.
/// Google rotates these slowly; an hour keeps the request path free of network
/// calls without letting a rotation go unnoticed for long.
const JWKS_TTL: Duration = Duration::from_secs(3600);

/// Floor between refreshes triggered by an unknown key id, so a stream of
/// tokens naming nonexistent keys cannot turn into a stream of outbound
/// requests to Google.
const UNKNOWN_KID_REFRESH_INTERVAL: Duration = Duration::from_secs(60);

/// Tolerance for clock skew between Google and this VM when checking `exp`.
const LEEWAY_SECS: u64 = 60;

#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    /// Base64url P-256 coordinates.
    x: String,
    y: String,
}

#[derive(Debug, Deserialize)]
struct JwkSet {
    keys: Vec<Jwk>,
}

/// The claims worth reading. IAP sets more; these are the ones that decide
/// whether to serve the request, plus the identity to log.
#[derive(Debug, Deserialize)]
struct IapClaims {
    /// Stable, unique, never reused: the right key for correlating actions.
    sub: String,
    /// Present for human callers. Absent for service accounts on some paths,
    /// which is why it is optional and used only for logging.
    email: Option<String>,
}

struct CachedKeys {
    keys: Vec<(String, DecodingKey)>,
    fetched_at: Instant,
    last_refresh_attempt: Instant,
}

/// Verifies IAP assertions against one expected audience.
///
/// One instance per role prefix: the audience is what separates them.
pub(crate) struct IapVerifier {
    audience: String,
    role: &'static str,
    http: reqwest::Client,
    jwks_url: String,
    keys: RwLock<Option<CachedKeys>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum IapError {
    #[error("missing IAP assertion")]
    MissingAssertion,
    #[error("malformed IAP assertion")]
    MalformedAssertion,
    #[error("IAP signing keys unavailable")]
    KeysUnavailable,
    #[error("unknown IAP signing key")]
    UnknownKey,
    #[error("IAP assertion rejected")]
    Rejected,
}

impl IapError {
    const fn status(&self) -> StatusCode {
        match self {
            // Every failure here is "you did not present a usable IAP
            // identity", which is a 401 regardless of why: the caller cannot
            // fix it by changing what they ask for, only by coming through
            // IAP. A 403 would wrongly imply the identity was understood and
            // found insufficient, which is IAP's decision, not this one.
            Self::MissingAssertion
            | Self::MalformedAssertion
            | Self::UnknownKey
            | Self::Rejected => StatusCode::UNAUTHORIZED,
            // The one case that is genuinely ours: we could not reach Google
            // to learn the keys, so we cannot judge. Fail closed, but say it
            // is temporary so a caller retries rather than re-authenticating.
            Self::KeysUnavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

impl IntoResponse for IapError {
    fn into_response(self) -> Response {
        // The body stays deliberately vague. A caller who failed to
        // authenticate learns nothing about which check failed; the detail
        // goes to the log, where an operator can see it.
        (self.status(), self.to_string()).into_response()
    }
}

impl IapVerifier {
    pub(crate) fn new(audience: String, role: &'static str) -> Self {
        Self {
            audience,
            role,
            http: reqwest::Client::new(),
            jwks_url: IAP_JWKS_URL.to_string(),
            keys: RwLock::new(None),
        }
    }

    #[cfg(test)]
    fn with_jwks_url(audience: String, role: &'static str, jwks_url: String) -> Self {
        Self {
            jwks_url,
            ..Self::new(audience, role)
        }
    }

    /// Returns the caller's stable subject id once the assertion checks out.
    async fn verify(&self, token: &str) -> Result<String, IapError> {
        let header = decode_header(token).map_err(|error| {
            warn!(target: "iap", role = self.role, %error, "Malformed IAP assertion header");
            IapError::MalformedAssertion
        })?;

        let kid = header.kid.ok_or_else(|| {
            warn!(target: "iap", role = self.role, "IAP assertion carries no key id");
            IapError::MalformedAssertion
        })?;

        let key = self.decoding_key(&kid).await?;

        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_audience(&[&self.audience]);
        validation.set_issuer(&[IAP_ISSUER]);
        validation.leeway = LEEWAY_SECS;
        // `exp` is what bounds a stolen token's usefulness, so its absence
        // must be a rejection rather than an unbounded token.
        validation.required_spec_claims = ["exp", "aud", "iss"]
            .into_iter()
            .map(String::from)
            .collect();

        let claims = decode::<IapClaims>(token, &key, &validation).map_err(|error| {
            // Includes the audience mismatch case: a token minted for another
            // role's backend lands here.
            warn!(
                target: "iap", role = self.role, %error,
                "IAP assertion failed validation"
            );
            IapError::Rejected
        })?;

        info!(
            target: "iap",
            role = self.role,
            subject = %claims.claims.sub,
            email = claims.claims.email.as_deref().unwrap_or("<none>"),
            "IAP assertion accepted"
        );

        Ok(claims.claims.sub)
    }

    async fn decoding_key(&self, kid: &str) -> Result<DecodingKey, IapError> {
        if let Some(key) = self.cached_key(kid).await {
            return Ok(key);
        }

        // Either the cache is cold, stale, or the token names a key we have
        // not seen. The last case is what a rotation looks like from here.
        self.refresh(kid).await?;

        self.cached_key(kid).await.ok_or_else(|| {
            warn!(target: "iap", role = self.role, kid, "IAP assertion names an unknown key");
            IapError::UnknownKey
        })
    }

    async fn cached_key(&self, kid: &str) -> Option<DecodingKey> {
        let guard = self.keys.read().await;
        let cached = guard.as_ref()?;

        if cached.fetched_at.elapsed() > JWKS_TTL {
            return None;
        }

        cached
            .keys
            .iter()
            .find(|(id, _)| id == kid)
            .map(|(_, key)| key.clone())
    }

    async fn refresh(&self, kid: &str) -> Result<(), IapError> {
        let mut guard = self.keys.write().await;

        // Another task may have refreshed while this one waited for the lock.
        if let Some(cached) = guard.as_ref() {
            let fresh = cached.fetched_at.elapsed() <= JWKS_TTL;
            if fresh && cached.keys.iter().any(|(id, _)| id == kid) {
                return Ok(());
            }
            if fresh && cached.last_refresh_attempt.elapsed() < UNKNOWN_KID_REFRESH_INTERVAL {
                // Recently refreshed and the key still is not there, so this
                // is a bad token rather than a rotation we have missed.
                return Ok(());
            }
        }

        let now = Instant::now();
        let fetched = self.fetch_keys().await;

        match fetched {
            Ok(keys) => {
                *guard = Some(CachedKeys {
                    keys,
                    fetched_at: now,
                    last_refresh_attempt: now,
                });
                Ok(())
            }
            Err(error) => {
                warn!(target: "iap", role = self.role, %error, "Could not fetch IAP signing keys");

                // Serving a stale key set beats refusing every request over a
                // transient failure to reach Google: the keys are still
                // Google's, and a signature that verifies against one is still
                // proof the token is genuine. Only a cold cache is fatal.
                match guard.as_mut() {
                    Some(cached) => {
                        cached.last_refresh_attempt = now;
                        Ok(())
                    }
                    None => Err(IapError::KeysUnavailable),
                }
            }
        }
    }

    async fn fetch_keys(&self) -> Result<Vec<(String, DecodingKey)>, reqwest::Error> {
        let set: JwkSet = self
            .http
            .get(&self.jwks_url)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;

        Ok(set
            .keys
            .into_iter()
            .filter_map(|jwk| {
                DecodingKey::from_ec_components(&jwk.x, &jwk.y)
                    .inspect_err(|error| {
                        warn!(
                            target: "iap", kid = %jwk.kid, %error,
                            "Skipping unusable IAP signing key"
                        );
                    })
                    .ok()
                    .map(|key| (jwk.kid, key))
            })
            .collect())
    }
}

/// Rejects any request that did not arrive through IAP for this role.
pub(crate) async fn require_iap(
    verifier: Arc<IapVerifier>,
    request: Request,
    next: Next,
) -> Result<Response, IapError> {
    let token = request
        .headers()
        .get(ASSERTION_HEADER)
        .ok_or_else(|| {
            // The VPC-internal case: something reached the bot without
            // passing the load balancer.
            warn!(
                target: "iap",
                role = verifier.role,
                path = %request.uri().path(),
                "Request carries no IAP assertion"
            );
            IapError::MissingAssertion
        })?
        .to_str()
        .map_err(|_| IapError::MalformedAssertion)?
        .to_string();

    verifier.verify(&token).await?;

    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
    use super::*;

    use axum::Router;
    use axum::body::Body;
    use axum::http::Request as HttpRequest;
    use axum::routing::get;
    use base64::Engine as _;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD as BASE64_URL;
    use httpmock::prelude::*;
    use jsonwebtoken::{EncodingKey, Header, encode};
    use p256::ecdsa::SigningKey;
    use p256::pkcs8::EncodePrivateKey;
    use serde::Serialize;
    use tower::ServiceExt as _;

    const TEST_KID: &str = "test-key";
    const READ_AUDIENCE: &str = "/projects/1/global/backendServices/11";
    const WRITE_AUDIENCE: &str = "/projects/1/global/backendServices/22";

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        email: String,
        aud: String,
        iss: String,
        exp: u64,
    }

    /// A P-256 keypair standing in for Google's: the JWK halves that go in the
    /// mocked key set, and the PEM that signs test tokens.
    struct TestKey {
        signing_pem: Vec<u8>,
        x: String,
        y: String,
    }

    fn test_key() -> TestKey {
        // Fixed bytes rather than a random key: a test that generates its own
        // key can pass while the code under test ignores the key entirely.
        let signing = SigningKey::from_bytes(&[7u8; 32].into()).expect("valid P-256 scalar");
        let public = signing.verifying_key().to_encoded_point(false);

        TestKey {
            signing_pem: signing
                .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                .expect("PEM encodes")
                .as_bytes()
                .to_vec(),
            x: BASE64_URL.encode(public.x().expect("uncompressed point has x")),
            y: BASE64_URL.encode(public.y().expect("uncompressed point has y")),
        }
    }

    fn token(key: &TestKey, audience: &str, issuer: &str, expires_in_secs: i64) -> String {
        let exp = u64::try_from(
            i64::try_from(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("after epoch")
                    .as_secs(),
            )
            .expect("fits i64")
                + expires_in_secs,
        )
        .expect("not before epoch");

        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(TEST_KID.to_string());

        encode(
            &header,
            &TestClaims {
                sub: "accounts.google.com:1234".to_string(),
                email: "operator@t0trade.com".to_string(),
                aud: audience.to_string(),
                iss: issuer.to_string(),
                exp,
            },
            &EncodingKey::from_ec_pem(&key.signing_pem).expect("PEM parses"),
        )
        .expect("token encodes")
    }

    fn jwks_server(key: &TestKey) -> MockServer {
        let server = MockServer::start();
        let body = serde_json::json!({
            "keys": [{
                "kid": TEST_KID,
                "kty": "EC",
                "crv": "P-256",
                "alg": "ES256",
                "x": key.x,
                "y": key.y,
            }]
        });

        server.mock(|when, then| {
            when.method(GET).path("/keys");
            then.status(200).json_body(body);
        });

        server
    }

    fn verifier(audience: &str, jwks: &MockServer) -> Arc<IapVerifier> {
        Arc::new(IapVerifier::with_jwks_url(
            audience.to_string(),
            "test",
            jwks.url("/keys"),
        ))
    }

    async fn call(verifier: Arc<IapVerifier>, header: Option<&str>) -> StatusCode {
        let app = Router::new()
            .route("/guarded", get(|| async { "ok" }))
            .layer(axum::middleware::from_fn(move |request, next| {
                let verifier = verifier.clone();
                async move { require_iap(verifier, request, next).await }
            }));

        let mut request = HttpRequest::builder().uri("/guarded");
        if let Some(value) = header {
            request = request.header(ASSERTION_HEADER, value);
        }

        app.oneshot(request.body(Body::empty()).expect("request builds"))
            .await
            .expect("router responds")
            .status()
    }

    #[tokio::test]
    async fn accepts_a_current_assertion_for_this_audience() {
        let key = test_key();
        let jwks = jwks_server(&key);

        let status = call(
            verifier(READ_AUDIENCE, &jwks),
            Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, 300)),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
    }

    /// The property the whole design rests on: IAP binds a token to the
    /// backend that admitted it, so a read-tier caller replaying their token
    /// against the write path must be refused even though the signature is
    /// perfectly valid.
    #[tokio::test]
    async fn rejects_a_token_minted_for_another_role() {
        let key = test_key();
        let jwks = jwks_server(&key);

        let status = call(
            verifier(WRITE_AUDIENCE, &jwks),
            Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, 300)),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// Anything reaching the bot from inside the VPC skips the load balancer,
    /// and with it IAP and the group check.
    #[tokio::test]
    async fn rejects_a_request_with_no_assertion() {
        let key = test_key();
        let jwks = jwks_server(&key);

        assert_eq!(
            call(verifier(READ_AUDIENCE, &jwks), None).await,
            StatusCode::UNAUTHORIZED
        );
    }

    #[tokio::test]
    async fn rejects_an_expired_assertion() {
        let key = test_key();
        let jwks = jwks_server(&key);

        let status = call(
            verifier(READ_AUDIENCE, &jwks),
            Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, -3600)),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// A correctly-signed token from any other Google issuer is not an IAP
    /// assertion and must not be treated as one.
    #[tokio::test]
    async fn rejects_an_assertion_from_another_issuer() {
        let key = test_key();
        let jwks = jwks_server(&key);

        let status = call(
            verifier(READ_AUDIENCE, &jwks),
            Some(&token(
                &key,
                READ_AUDIENCE,
                "https://accounts.google.com",
                300,
            )),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn rejects_a_token_signed_by_a_foreign_key() {
        let google = test_key();
        let jwks = jwks_server(&google);

        let impostor = TestKey {
            signing_pem: SigningKey::from_bytes(&[9u8; 32].into())
                .expect("valid P-256 scalar")
                .to_pkcs8_pem(p256::pkcs8::LineEnding::LF)
                .expect("PEM encodes")
                .as_bytes()
                .to_vec(),
            x: google.x.clone(),
            y: google.y.clone(),
        };

        let status = call(
            verifier(READ_AUDIENCE, &jwks),
            Some(&token(&impostor, READ_AUDIENCE, IAP_ISSUER, 300)),
        )
        .await;

        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }

    /// A cold cache plus an unreachable key endpoint is the one case we cannot
    /// judge, and it must fail closed rather than serving the request.
    #[tokio::test]
    async fn fails_closed_when_the_keys_cannot_be_fetched() {
        let key = test_key();
        let unreachable = MockServer::start();
        unreachable.mock(|when, then| {
            when.method(GET).path("/keys");
            then.status(500);
        });

        let status = call(
            verifier(READ_AUDIENCE, &unreachable),
            Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, 300)),
        )
        .await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}
