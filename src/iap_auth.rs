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
    keys: Vec<serde_json::Value>,
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
}

/// Verifies IAP assertions against one expected audience.
///
/// One instance per role prefix: the audience is what separates them.
pub(crate) struct IapVerifier {
    role: &'static str,
    http: reqwest::Client,
    jwks_url: String,
    /// Built once at construction: audience, issuer, and required claims
    /// never change per request.
    validation: Validation,
    keys: RwLock<Option<CachedKeys>>,
    /// When the last outbound JWKS fetch was STARTED, successful or not.
    /// Lives outside `CachedKeys` so the refresh floor also covers a cold
    /// cache during an outage (a per-request fetch storm otherwise) and can
    /// be claimed without holding the key lock across the network call.
    /// A std Mutex: only ever held for a read-modify-write, never across an
    /// await.
    last_refresh_attempt: std::sync::Mutex<Option<Instant>>,
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
        // goes to the log, where an operator can see it. JSON, because the
        // handlers behind this gate answer {"error": ...} and ops tooling
        // should parse one shape for the whole route family.
        (
            self.status(),
            axum::Json(serde_json::json!({ "error": self.to_string() })),
        )
            .into_response()
    }
}

impl IapVerifier {
    /// `http` is shared between the per-role verifiers (reqwest::Client is a
    /// cheap Arc clone) and MUST carry timeouts: the JWKS fetch otherwise has
    /// no bound, and an unbounded fetch during a Google outage would pin the
    /// refresh slot. See [`ops_api_routes`] for the one place it is built.
    pub(crate) fn new(audience: String, role: &'static str, http: reqwest::Client) -> Self {
        let mut validation = Validation::new(Algorithm::ES256);
        validation.set_audience(&[&audience]);
        validation.set_issuer(&[IAP_ISSUER]);
        validation.leeway = LEEWAY_SECS;
        // `exp` is what bounds a stolen token's usefulness, so its absence
        // must be a rejection rather than an unbounded token.
        validation.required_spec_claims = ["exp", "aud", "iss"]
            .into_iter()
            .map(String::from)
            .collect();

        Self {
            role,
            http,
            jwks_url: IAP_JWKS_URL.to_string(),
            validation,
            keys: RwLock::new(None),
            last_refresh_attempt: std::sync::Mutex::new(None),
        }
    }

    #[cfg(test)]
    fn with_jwks_url(audience: String, role: &'static str, jwks_url: String) -> Self {
        Self {
            jwks_url,
            ..Self::new(audience, role, reqwest::Client::new())
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

        let claims = decode::<IapClaims>(token, &key, &self.validation).map_err(|error| {
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
        if let Some(key) = self.cached_key(kid, false).await {
            return Ok(key);
        }

        // Either the cache is cold, stale, or the token names a key we have
        // not seen. The last case is what a rotation looks like from here.
        self.refresh(kid).await?;

        // Accept whatever the cache holds now, stale included: a retained key
        // is still Google's, and a signature verifying against it is still
        // proof of a genuine token. Staleness drives the refresh cadence
        // above; it is never by itself a reason to reject.
        self.cached_key(kid, true).await.ok_or_else(|| {
            warn!(target: "iap", role = self.role, kid, "IAP assertion names an unknown key");
            IapError::UnknownKey
        })
    }

    async fn cached_key(&self, kid: &str, allow_stale: bool) -> Option<DecodingKey> {
        let guard = self.keys.read().await;

        // Cloning the key out and dropping the guard before returning keeps
        // the read lock held for the lookup only, so a refresh waiting for the
        // write lock is not queued behind a caller that has already finished
        // reading.
        let key = guard.as_ref().and_then(|cached| {
            if !allow_stale && cached.fetched_at.elapsed() > JWKS_TTL {
                return None;
            }

            cached
                .keys
                .iter()
                .find(|(id, _)| id == kid)
                .map(|(_, key)| key.clone())
        });

        drop(guard);

        key
    }

    async fn refresh(&self, kid: &str) -> Result<(), IapError> {
        // Another task may have refreshed while this one was on its way
        // here. Read coldness in the same pass: the throttle branch below
        // needs it, and reading it there would put an await under the
        // attempt guard (a std Mutex, which must never be held across an
        // await; the future would not even be Send).
        let cache_is_cold = {
            let guard = self.keys.read().await;
            if let Some(cached) = guard.as_ref() {
                if cached.fetched_at.elapsed() <= JWKS_TTL
                    && cached.keys.iter().any(|(id, _)| id == kid)
                {
                    return Ok(());
                }
            }
            guard.is_none()
        };

        // Claim the single refresh slot or yield to the floor: one outbound
        // request per interval, whatever the trigger. An unknown kid on a
        // fresh set is a bad token rather than a rotation we have missed; a
        // stale set (or a COLD cache) during a Google outage must not turn
        // every request into a fetch. Retained keys keep serving meanwhile.
        {
            let mut attempt = self
                .last_refresh_attempt
                .lock()
                .map_err(|_| IapError::KeysUnavailable)?;
            if attempt.is_some_and(|at| at.elapsed() < UNKNOWN_KID_REFRESH_INTERVAL) {
                // Throttled. With retained keys the caller's follow-up lookup
                // serves them; with a cold cache there is nothing to judge
                // against, which is the KeysUnavailable case, not a claim
                // that the token's key is unknown. (`cache_is_cold` is a few
                // instructions stale; the worst case is one 503 for a
                // request racing the cache warming, healed on its retry.)
                if cache_is_cold {
                    return Err(IapError::KeysUnavailable);
                }
                return Ok(());
            }
            *attempt = Some(Instant::now());
        }

        // The fetch deliberately runs with NO lock held: a slow or hung
        // fetch must not block readers away from the retained keys (that
        // would defeat the stale-serving policy below), and the client's
        // timeouts bound the slot claimed above.
        match self.fetch_keys().await {
            Ok(keys) => {
                *self.keys.write().await = Some(CachedKeys {
                    keys,
                    fetched_at: Instant::now(),
                });
                Ok(())
            }
            Err(error) => {
                warn!(target: "iap", role = self.role, %error, "Could not fetch IAP signing keys");

                // Serving a stale key set beats refusing every request over a
                // transient failure to reach Google: the keys are still
                // Google's, and a signature that verifies against one is still
                // proof the token is genuine. Only a cold cache is fatal.
                if self.keys.read().await.is_some() {
                    Ok(())
                } else {
                    Err(IapError::KeysUnavailable)
                }
            }
        }
    }

    async fn fetch_keys(&self) -> Result<Vec<(String, DecodingKey)>, reqwest::Error> {
        // Entries are parsed one by one, not as a typed Vec<Jwk>: a single
        // entry this code cannot use (an RSA key, a key without x/y) must be
        // SKIPPED, not fail deserialization of the whole document and take
        // every valid key down with it.
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
            .filter_map(|entry| {
                let jwk: Jwk = serde_json::from_value(entry)
                    .inspect_err(|error| {
                        warn!(target: "iap", %error, "Skipping non-EC IAP key entry");
                    })
                    .ok()?;
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
        .map_err(|_| {
            warn!(
                target: "iap",
                role = verifier.role,
                "IAP assertion header is not valid UTF-8"
            );
            IapError::MalformedAssertion
        })?;

    verifier.verify(token).await?;

    Ok(next.run(request).await)
}

#[cfg(test)]
mod tests {
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

    use super::*;

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

    /// Seeds the verifier's cache as if the keys were fetched two TTLs ago,
    /// with the refresh floor long expired so a refresh attempt is permitted.
    async fn seed_stale_cache(verifier: &IapVerifier, key: &TestKey) {
        let decoding = DecodingKey::from_ec_components(&key.x, &key.y).expect("test key converts");
        let long_ago = Instant::now()
            .checked_sub(JWKS_TTL * 2)
            .expect("process uptime is irrelevant to a monotonic clock epoch");

        *verifier.keys.write().await = Some(CachedKeys {
            keys: vec![(TEST_KID.to_string(), decoding)],
            fetched_at: long_ago,
        });
        *verifier.last_refresh_attempt.lock().expect("not poisoned") = Some(long_ago);
    }

    /// The stated policy is that a stale key set beats refusing every request
    /// over a transient failure to reach Google: after a failed refresh the
    /// retained keys must actually be SERVED, not merely retained.
    #[tokio::test]
    async fn serves_the_retained_keys_when_a_stale_cache_cannot_be_refreshed() {
        let key = test_key();
        let unreachable = MockServer::start();
        let mock = unreachable.mock(|when, then| {
            when.method(GET).path("/keys");
            then.status(500);
        });

        let iap = verifier(READ_AUDIENCE, &unreachable);
        seed_stale_cache(&iap, &key).await;

        let status = call(
            iap.clone(),
            Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, 300)),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        // The staleness did trigger a refresh attempt; the failure just did
        // not take the retained keys down with it.
        mock.assert_hits(1);
    }

    /// A Google outage on a stale cache must not turn every request into an
    /// outbound fetch: the refresh floor applies to stale sets exactly as it
    /// does to unknown-kid streams.
    #[tokio::test]
    async fn throttles_refreshes_while_serving_a_stale_cache() {
        let key = test_key();
        let unreachable = MockServer::start();
        let mock = unreachable.mock(|when, then| {
            when.method(GET).path("/keys");
            then.status(500);
        });

        let iap = verifier(READ_AUDIENCE, &unreachable);
        seed_stale_cache(&iap, &key).await;

        for _ in 0..3 {
            let status = call(
                iap.clone(),
                Some(&token(&key, READ_AUDIENCE, IAP_ISSUER, 300)),
            )
            .await;
            assert_eq!(status, StatusCode::OK);
        }

        // The first request's failed refresh stamps last_refresh_attempt; the
        // two that follow inside the interval serve the stale keys without
        // going back to Google.
        mock.assert_hits(1);
    }
}
