//! GCP KMS-backed Turnkey API stamper — keyless Turnkey authentication.
//!
//! Turnkey authenticates every request with an `X-Stamp` header: an ECDSA
//! P-256 signature (DER, over SHA-256 of the exact request body) produced
//! by an "API key" whose PUBLIC half is registered with the Turnkey org.
//! Nothing requires the private half to exist as material: this stamper
//! holds it as a non-extractable `EC_SIGN_P256_SHA256` key in Cloud KMS
//! and produces stamps via `AsymmetricSign`, authorized by the caller's
//! ambient GCP identity (the GCE VM's attached service account) instead
//! of a stored secret. Consequences:
//!
//!   - the `[wallet]` secrets entry disappears — there is no
//!     `api_private_key` to store, rotate, or exfiltrate;
//!   - IAM (`roles/cloudkms.signerVerifier` on the key) decides who can
//!     authenticate to Turnkey, and every stamp lands in the KMS
//!     data-access audit log;
//!   - per-environment isolation is a per-env key + per-env Turnkey API
//!     user, not a shared credential.
//!
//! Trust hygiene mirrors [`turnkey`](super::turnkey)'s
//! recovered-address check: the DER signature KMS returns is verified
//! against the key's cached public half over the exact body before it is
//! ever sent — a stamp we cannot verify is refused, so a confused or
//! substituted KMS response cannot make us authenticate garbage.

use alloy::primitives::hex;
use base64::Engine as _;
use base64::prelude::{BASE64_STANDARD, BASE64_URL_SAFE_NO_PAD};
use p256::ecdsa::signature::Verifier;
use p256::ecdsa::{Signature as P256Signature, VerifyingKey};
use p256::elliptic_curve::sec1::ToEncodedPoint;
use p256::pkcs8::DecodePublicKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::info;
use turnkey_api_key_stamper::StampHeader;

/// Header name and scheme string from Turnkey's stamp specification.
/// Mirrors `turnkey_api_key_stamper`'s constants; duplicated locally so
/// this module depends only on the crate's public `StampHeader` type.
const STAMP_HEADER_NAME: &str = "X-Stamp";
const SIGNATURE_SCHEME_P256: &str = "SIGNATURE_SCHEME_TK_API_P256";

const DEFAULT_KMS_BASE_URL: &str = "https://cloudkms.googleapis.com/v1";
const METADATA_TOKEN_URL: &str =
    "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token";

/// The algorithm the KMS key version must carry: ECDSA on P-256 over a
/// SHA-256 digest — exactly what a Turnkey P-256 API-key stamp is.
const REQUIRED_ALGORITHM: &str = "EC_SIGN_P256_SHA256";

/// Errors from constructing or using the KMS stamper. HTTP error bodies
/// are included only for non-2xx responses (Google error JSON — no
/// tokens or signatures); success bodies are never embedded in errors.
#[derive(Debug, thiserror::Error)]
pub enum GcpKmsStamperError {
    #[error("KMS HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),
    #[error("KMS returned HTTP {status}: {body}")]
    UnexpectedStatus { status: u16, body: String },
    #[error("token endpoint returned HTTP {status}")]
    TokenStatus { status: u16 },
    #[error(
        "KMS key version {key_version} has algorithm {algorithm}; Turnkey stamping requires {REQUIRED_ALGORITHM}"
    )]
    WrongAlgorithm {
        key_version: String,
        algorithm: String,
    },
    #[error("failed to parse KMS public key PEM: {0}")]
    PublicKeyParse(String),
    #[error("failed to decode KMS base64: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("KMS signature is not valid DER ECDSA-P256: {0}")]
    SignatureDer(String),
    #[error(
        "KMS signature does not verify over the request body with the key's public half -- \
         refusing to send a stamp we cannot verify"
    )]
    SignatureVerification,
    #[error("stamp serialization failed: {0}")]
    Json(#[from] serde_json::Error),
}

/// Where the Google OAuth access token for KMS calls comes from.
#[derive(Debug, Clone)]
enum AccessTokenSource {
    /// `GOOGLE_OAUTH_ACCESS_TOKEN` env var when set (human/local runs),
    /// otherwise the GCE metadata server (the VM's ambient identity).
    Ambient,
    /// Fixed token endpoint — tests only.
    #[cfg(test)]
    Endpoint(String),
}

/// JSON shape of the `X-Stamp` value (camelCase per Turnkey's spec —
/// same shape `turnkey_api_key_stamper` produces).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TurnkeyApiStamp {
    public_key: String,
    signature: String,
    scheme: String,
}

#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
}

#[derive(Deserialize)]
struct PublicKeyResponse {
    pem: String,
    algorithm: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct AsymmetricSignResponse {
    signature: String,
}

#[derive(Serialize)]
struct AsymmetricSignRequest<'a> {
    digest: DigestBody<'a>,
}

#[derive(Serialize)]
struct DigestBody<'a> {
    sha256: &'a str,
}

/// Stamps Turnkey requests by signing with a Cloud KMS P-256 key.
pub struct GcpKmsStamper {
    http: reqwest::Client,
    kms_base_url: String,
    token_source: AccessTokenSource,
    /// Full KMS key-version resource name
    /// (`projects/.../cryptoKeyVersions/N`).
    key_version: String,
    /// Compressed SEC1 public key, hex — cached at construction, echoed
    /// in every stamp, and used to verify KMS's signatures before use.
    public_key_hex: String,
    verifying_key: VerifyingKey,
}

impl std::fmt::Debug for GcpKmsStamper {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("GcpKmsStamper")
            .field("key_version", &self.key_version)
            .field("public_key", &self.public_key_hex)
            .finish_non_exhaustive()
    }
}

impl GcpKmsStamper {
    /// Constructs a stamper for the given KMS key version, fetching and
    /// caching its public key (and validating the algorithm) up front so
    /// a misconfigured key fails at startup, not on the first signature.
    pub async fn new(key_version: String) -> Result<Self, GcpKmsStamperError> {
        Self::with_endpoints(
            key_version,
            DEFAULT_KMS_BASE_URL.to_string(),
            AccessTokenSource::Ambient,
        )
        .await
    }

    async fn with_endpoints(
        key_version: String,
        kms_base_url: String,
        token_source: AccessTokenSource,
    ) -> Result<Self, GcpKmsStamperError> {
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(20))
            .user_agent("st0x-turnkey-kms-stamper")
            .build()?;

        let token = Self::access_token(&http, &token_source).await?;
        let url = format!("{kms_base_url}/{key_version}/publicKey");
        let response = http.get(&url).bearer_auth(&token).send().await?;
        let status = response.status();
        if !status.is_success() {
            return Err(GcpKmsStamperError::UnexpectedStatus {
                status: status.as_u16(),
                body: response.text().await.unwrap_or_default(),
            });
        }
        let PublicKeyResponse { pem, algorithm } = response.json().await?;

        if algorithm != REQUIRED_ALGORITHM {
            return Err(GcpKmsStamperError::WrongAlgorithm {
                key_version,
                algorithm,
            });
        }

        let public_key = p256::PublicKey::from_public_key_pem(&pem)
            .map_err(|error| GcpKmsStamperError::PublicKeyParse(error.to_string()))?;
        let public_key_hex = hex::encode(public_key.to_encoded_point(true).as_bytes());
        let verifying_key = VerifyingKey::from(&public_key);

        info!(
            target: "wallet",
            key_version = %key_version,
            public_key = %public_key_hex,
            "Turnkey KMS stamper initialized (keyless authentication)"
        );

        Ok(Self {
            http,
            kms_base_url,
            token_source,
            key_version,
            public_key_hex,
            verifying_key,
        })
    }

    async fn access_token(
        http: &reqwest::Client,
        source: &AccessTokenSource,
    ) -> Result<String, GcpKmsStamperError> {
        let token_url = match source {
            AccessTokenSource::Ambient => {
                // Human/local runs (integration tests, break-glass CLI use)
                // carry a token in the environment; the VM uses its
                // metadata server.
                if let Ok(token) = std::env::var("GOOGLE_OAUTH_ACCESS_TOKEN") {
                    return Ok(token);
                }
                METADATA_TOKEN_URL
            }
            #[cfg(test)]
            AccessTokenSource::Endpoint(url) => url.as_str(),
        };

        let response = http
            .get(token_url)
            .header("Metadata-Flavor", "Google")
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(GcpKmsStamperError::TokenStatus {
                status: status.as_u16(),
            });
        }
        let TokenResponse { access_token } = response.json().await?;
        Ok(access_token)
    }

    /// Produces the `X-Stamp` header for `body`: SHA-256 the body, have
    /// KMS sign the digest, verify the returned signature against the
    /// cached public key, and encode Turnkey's stamp JSON.
    pub async fn stamp(&self, body: &[u8]) -> Result<StampHeader, GcpKmsStamperError> {
        let digest = Sha256::digest(body);
        let digest_b64 = BASE64_STANDARD.encode(digest);

        let token = Self::access_token(&self.http, &self.token_source).await?;
        let url = format!("{}/{}:asymmetricSign", self.kms_base_url, self.key_version);
        let response = self
            .http
            .post(&url)
            .bearer_auth(&token)
            .json(&AsymmetricSignRequest {
                digest: DigestBody {
                    sha256: &digest_b64,
                },
            })
            .send()
            .await?;
        let status = response.status();
        if !status.is_success() {
            return Err(GcpKmsStamperError::UnexpectedStatus {
                status: status.as_u16(),
                body: response.text().await.unwrap_or_default(),
            });
        }
        let AsymmetricSignResponse { signature } = response.json().await?;

        let der = BASE64_STANDARD.decode(signature)?;
        let parsed = P256Signature::from_der(&der)
            .map_err(|error| GcpKmsStamperError::SignatureDer(error.to_string()))?;

        // Verify over the BODY (Verifier hashes with SHA-256 internally,
        // matching what KMS signed) with the public key cached at
        // construction — the same key Turnkey will verify against.
        self.verifying_key
            .verify(body, &parsed)
            .map_err(|_| GcpKmsStamperError::SignatureVerification)?;

        let stamp = TurnkeyApiStamp {
            public_key: self.public_key_hex.clone(),
            signature: hex::encode(&der),
            scheme: SIGNATURE_SCHEME_P256.to_string(),
        };
        let stamp_json = serde_json::to_string(&stamp)?;

        Ok(StampHeader {
            name: STAMP_HEADER_NAME.to_string(),
            value: BASE64_URL_SAFE_NO_PAD.encode(stamp_json.as_bytes()),
        })
    }
}

#[cfg(test)]
mod tests {
    use httpmock::MockServer;
    use p256::ecdsa::SigningKey;
    use p256::ecdsa::signature::hazmat::PrehashSigner;
    use p256::elliptic_curve::rand_core::OsRng;
    use p256::pkcs8::EncodePublicKey;

    use super::*;

    const KEY_VERSION: &str = "projects/p/locations/l/keyRings/r/cryptoKeys/k/cryptoKeyVersions/1";

    fn test_signing_key() -> SigningKey {
        SigningKey::random(&mut OsRng)
    }

    fn public_key_pem(key: &SigningKey) -> String {
        key.verifying_key()
            .to_public_key_pem(p256::pkcs8::LineEnding::default())
            .expect("PEM encoding of a valid P-256 key cannot fail")
    }

    /// Mocks the token endpoint plus KMS publicKey GET for `key`.
    fn mock_key_endpoints(server: &MockServer, key: &SigningKey, algorithm: &str) {
        server.mock(|when, then| {
            when.method("GET").path("/token");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "access_token": "test-token",
                    "expires_in": 3599,
                    "token_type": "Bearer",
                }));
        });
        server.mock(|when, then| {
            when.method("GET").path(format!("/{KEY_VERSION}/publicKey"));
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "pem": public_key_pem(key),
                    "algorithm": algorithm,
                }));
        });
    }

    async fn stamper_for(server: &MockServer) -> GcpKmsStamper {
        GcpKmsStamper::with_endpoints(
            KEY_VERSION.to_string(),
            server.base_url(),
            AccessTokenSource::Endpoint(format!("{}/token", server.base_url())),
        )
        .await
        .expect("stamper construction against mocks should succeed")
    }

    /// Pre-computes the deterministic (RFC 6979) signature KMS would
    /// return for `body` and mocks the asymmetricSign endpoint with it.
    fn mock_sign_endpoint(server: &MockServer, key: &SigningKey, signed_body: &[u8]) {
        let digest = Sha256::digest(signed_body);
        let signature: P256Signature = key
            .sign_prehash(&digest)
            .expect("signing a 32-byte digest with a valid key cannot fail");
        server.mock(|when, then| {
            when.method("POST")
                .path(format!("/{KEY_VERSION}:asymmetricSign"));
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({
                    "name": KEY_VERSION,
                    "signature": BASE64_STANDARD.encode(signature.to_der()),
                }));
        });
    }

    #[tokio::test]
    async fn stamp_produces_verifiable_turnkey_stamp() {
        let key = test_signing_key();
        let server = MockServer::start();
        mock_key_endpoints(&server, &key, REQUIRED_ALGORITHM);

        let body = br#"{"type":"ACTIVITY_TYPE_SIGN_TRANSACTION_V2"}"#;
        mock_sign_endpoint(&server, &key, body);

        let stamper = stamper_for(&server).await;
        let StampHeader { name, value } = stamper.stamp(body).await.expect("stamp should succeed");

        assert_eq!(name, "X-Stamp");

        let decoded = BASE64_URL_SAFE_NO_PAD
            .decode(value)
            .expect("stamp value must be base64url");
        let stamp: serde_json::Value =
            serde_json::from_slice(&decoded).expect("stamp must be JSON");

        assert_eq!(stamp["scheme"], "SIGNATURE_SCHEME_TK_API_P256");
        assert_eq!(
            stamp["publicKey"],
            hex::encode(key.verifying_key().to_encoded_point(true).as_bytes())
        );

        // The stamp's signature must verify over the exact body with the
        // advertised public key — what Turnkey's server will check.
        let der = hex::decode(
            stamp["signature"]
                .as_str()
                .expect("signature must be a string"),
        )
        .expect("signature must be hex");
        let signature = P256Signature::from_der(&der).expect("signature must be DER");
        key.verifying_key()
            .verify(body, &signature)
            .expect("stamp signature must verify over the body");
    }

    #[tokio::test]
    async fn stamp_rejects_signature_over_different_content() {
        let key = test_signing_key();
        let server = MockServer::start();
        mock_key_endpoints(&server, &key, REQUIRED_ALGORITHM);

        // KMS (or a substituted response) signs DIFFERENT content than
        // the body being stamped — the pre-send verification must refuse.
        mock_sign_endpoint(&server, &key, b"something else entirely");

        let stamper = stamper_for(&server).await;
        let error = stamper
            .stamp(b"the actual request body")
            .await
            .expect_err("mismatched signature must be refused");

        assert!(matches!(error, GcpKmsStamperError::SignatureVerification));
    }

    #[tokio::test]
    async fn construction_rejects_wrong_algorithm() {
        let key = test_signing_key();
        let server = MockServer::start();
        mock_key_endpoints(&server, &key, "EC_SIGN_SECP256K1_SHA256");

        let error = GcpKmsStamper::with_endpoints(
            KEY_VERSION.to_string(),
            server.base_url(),
            AccessTokenSource::Endpoint(format!("{}/token", server.base_url())),
        )
        .await
        .expect_err("wrong algorithm must fail at construction");

        assert!(matches!(
            error,
            GcpKmsStamperError::WrongAlgorithm { algorithm, .. }
                if algorithm == "EC_SIGN_SECP256K1_SHA256"
        ));
    }

    #[tokio::test]
    async fn construction_surfaces_kms_error_status() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method("GET").path("/token");
            then.status(200)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({ "access_token": "t" }));
        });
        server.mock(|when, then| {
            when.method("GET").path(format!("/{KEY_VERSION}/publicKey"));
            then.status(403)
                .header("Content-Type", "application/json")
                .json_body(serde_json::json!({ "error": { "status": "PERMISSION_DENIED" } }));
        });

        let error = GcpKmsStamper::with_endpoints(
            KEY_VERSION.to_string(),
            server.base_url(),
            AccessTokenSource::Endpoint(format!("{}/token", server.base_url())),
        )
        .await
        .expect_err("403 must surface");

        assert!(matches!(
            error,
            GcpKmsStamperError::UnexpectedStatus { status: 403, .. }
        ));
    }

    // --- Integration test (real Cloud KMS) --------------------------
    //
    // Verifies the stamper against a REAL KMS key end-to-end (construct,
    // stamp, self-verify) without touching Turnkey. Required env vars:
    //   GOOGLE_OAUTH_ACCESS_TOKEN  -- `gcloud auth print-access-token`
    //   TURNKEY_KMS_KEY_VERSION    -- full cryptoKeyVersions resource name
    //                                 of an EC_SIGN_P256_SHA256 key

    #[ignore = "requires GOOGLE_OAUTH_ACCESS_TOKEN + TURNKEY_KMS_KEY_VERSION -- run with `cargo test -- --ignored`"]
    #[tokio::test]
    async fn kms_integration() {
        let key_version =
            std::env::var("TURNKEY_KMS_KEY_VERSION").expect("TURNKEY_KMS_KEY_VERSION must be set");

        let stamper = GcpKmsStamper::new(key_version)
            .await
            .expect("stamper construction against real KMS should succeed");

        let StampHeader { name, .. } = stamper
            .stamp(b"integration test body")
            .await
            .expect("stamping via real KMS should succeed (self-verified)");

        assert_eq!(name, "X-Stamp");
    }
}
