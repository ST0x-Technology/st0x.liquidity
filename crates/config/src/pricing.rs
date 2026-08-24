//! Pricing-service configuration and secret assembly.

use serde::Deserialize;
use url::{Host, Url};

/// Non-secret pricing-service transport settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    pub ws_url: Url,
    /// Authenticate with a Google ID token minted from the VM's ambient
    /// service-account identity (Cloud Run IAM) instead of a static
    /// `[pricing].api_key` secret — the pricing plane on Cloud Run runs
    /// with app-level key auth OFF and IAM as the sole gate. Mirrors the
    /// `kms_api_key` precedent: the credential is the machine's identity,
    /// so nothing is stored, rotated, or shared. Mutually exclusive with
    /// the secret; requires a wss URL.
    #[serde(default)]
    pub gcp_id_token: bool,
}

/// Pricing-service Bearer credential from the encrypted secrets file.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PricingSecrets {
    api_key: String,
}

/// Redacted pricing-service Bearer credential.
#[derive(Clone)]
pub struct PricingApiKey(String);

impl PricingApiKey {
    /// Returns the credential for the immediate construction of the
    /// `Authorization` header. The returned value must never be logged.
    #[must_use]
    pub fn bearer_value(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for PricingApiKey {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("PricingApiKey(<redacted>)")
    }
}

/// How the pricing WS session authenticates.
#[derive(Clone, Debug)]
pub enum PricingAuth {
    /// Static Bearer credential from the secrets file (droplet-local
    /// pricing with `PRICING_API_KEYS` app auth).
    ApiKey(PricingApiKey),
    /// Google ID token minted per-connect from the instance metadata
    /// server, `audience` = the pricing service's https origin (Cloud Run
    /// IAM). Tokens expire hourly; the session mints a fresh one on every
    /// (re)connect, which the reconnect loop already guarantees.
    GcpIdToken { audience: String },
}

/// Runtime context for the dashboard's pricing-service subscription.
#[derive(Clone, Debug)]
pub struct PricingCtx {
    pub ws_url: Url,
    pub auth: PricingAuth,
}

impl PricingCtx {
    /// Builds a pricing context from explicit runtime inputs.
    pub fn new(ws_url: Url, api_key: String) -> Result<Self, PricingCtxError> {
        Self::assemble(
            Some(PricingConfig {
                ws_url,
                gcp_id_token: false,
            }),
            Some(PricingSecrets { api_key }),
            true,
        )?
        .ok_or(PricingCtxError::MissingConfig)
    }

    pub(crate) fn assemble(
        config: Option<PricingConfig>,
        secrets: Option<PricingSecrets>,
        required: bool,
    ) -> Result<Option<Self>, PricingCtxError> {
        let Some(config) = config else {
            return match (secrets, required) {
                (None, false) => Ok(None),
                _ => Err(PricingCtxError::MissingConfig),
            };
        };

        if !matches!(config.ws_url.scheme(), "ws" | "wss") {
            return Err(PricingCtxError::InvalidWebSocketScheme);
        }
        if config.ws_url.scheme() == "ws" && !allows_plaintext_ws(&config.ws_url) {
            return Err(PricingCtxError::InsecureWebSocketUrl);
        }
        if !config.ws_url.username().is_empty()
            || config.ws_url.password().is_some()
            || config.ws_url.query().is_some()
            || config.ws_url.fragment().is_some()
        {
            return Err(PricingCtxError::CredentialsInWebSocketUrl);
        }

        let auth = match (config.gcp_id_token, secrets) {
            // Both set: refuse rather than guess, exactly like the wallet's
            // AmbiguousCredentials -- silent precedence would let a stale
            // secret shadow the intended identity (or vice versa).
            (true, Some(_)) => return Err(PricingCtxError::AmbiguousAuth),
            (true, None) => {
                // A Google ID token only means anything over TLS to the
                // audience it names; a plaintext ws URL would leak it.
                if config.ws_url.scheme() != "wss" {
                    return Err(PricingCtxError::IdTokenRequiresWss);
                }
                let host = config
                    .ws_url
                    .host_str()
                    .ok_or(PricingCtxError::InvalidWebSocketScheme)?;
                PricingAuth::GcpIdToken {
                    audience: format!("https://{host}"),
                }
            }
            (false, Some(secrets)) => {
                if secrets.api_key.trim().is_empty() {
                    return Err(PricingCtxError::EmptyApiKey);
                }
                if !secrets.api_key.bytes().all(|byte| byte.is_ascii_graphic()) {
                    return Err(PricingCtxError::InvalidApiKey);
                }
                PricingAuth::ApiKey(PricingApiKey(secrets.api_key))
            }
            (false, None) => return Err(PricingCtxError::MissingSecrets),
        };

        Ok(Some(Self {
            ws_url: config.ws_url,
            auth,
        }))
    }
}

fn allows_plaintext_ws(url: &Url) -> bool {
    match url.host() {
        Some(Host::Domain(host)) => matches!(host, "st0x-pricing" | "localhost"),
        Some(Host::Ipv4(address)) => address.is_loopback(),
        Some(Host::Ipv6(address)) => address.is_loopback(),
        None => false,
    }
}

/// Invalid or incomplete pricing-service configuration.
#[derive(Debug, thiserror::Error)]
pub enum PricingCtxError {
    #[error("[pricing] config is required when equity assets are configured")]
    MissingConfig,
    #[error("[pricing] secrets are required when equity assets are configured")]
    MissingSecrets,
    #[error("[pricing].ws_url must use the ws or wss scheme")]
    InvalidWebSocketScheme,
    #[error(
        "[pricing].ws_url must use wss unless it targets st0x-pricing, localhost, or a loopback address"
    )]
    InsecureWebSocketUrl,
    #[error("[pricing].ws_url must not contain userinfo, query parameters, or fragments")]
    CredentialsInWebSocketUrl,
    #[error("[pricing].api_key must not be empty")]
    EmptyApiKey,
    #[error("[pricing].api_key must contain only visible ASCII characters")]
    InvalidApiKey,
    #[error(
        "[pricing] has BOTH gcp_id_token and an api_key secret -- refusing to guess; remove one"
    )]
    AmbiguousAuth,
    #[error("[pricing].gcp_id_token requires a wss:// ws_url")]
    IdTokenRequiresWss,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> PricingConfig {
        PricingConfig {
            ws_url: Url::parse(url).unwrap(),
            gcp_id_token: false,
        }
    }

    fn gcp_config(url: &str) -> PricingConfig {
        PricingConfig {
            ws_url: Url::parse(url).unwrap(),
            gcp_id_token: true,
        }
    }

    fn secrets(api_key: &str) -> PricingSecrets {
        PricingSecrets {
            api_key: api_key.to_string(),
        }
    }

    #[test]
    fn equity_assets_require_both_pricing_sections() {
        assert!(matches!(
            PricingCtx::assemble(None, Some(secrets("key")), true),
            Err(PricingCtxError::MissingConfig)
        ));
        assert!(matches!(
            PricingCtx::assemble(Some(config("wss://pricing.test/ws")), None, true),
            Err(PricingCtxError::MissingSecrets)
        ));
    }

    #[test]
    fn empty_asset_sets_need_no_pricing_connection() {
        assert!(PricingCtx::assemble(None, None, false).unwrap().is_none());
    }

    #[test]
    fn websocket_url_rejects_http_scheme() {
        assert!(matches!(
            PricingCtx::assemble(
                Some(config("https://pricing.test/ws")),
                Some(secrets("key")),
                true
            ),
            Err(PricingCtxError::InvalidWebSocketScheme)
        ));
    }

    #[test]
    fn websocket_url_rejects_remote_plaintext_transport() {
        assert!(matches!(
            PricingCtx::assemble(
                Some(config("ws://pricing.test/ws")),
                Some(secrets("key")),
                true
            ),
            Err(PricingCtxError::InsecureWebSocketUrl)
        ));
    }

    #[test]
    fn websocket_url_allows_private_plaintext_transports() {
        for url in [
            "ws://st0x-pricing:8080/ws",
            "ws://localhost:8080/ws",
            "ws://127.0.0.1:8080/ws",
            "ws://[::1]:8080/ws",
        ] {
            assert!(
                PricingCtx::assemble(Some(config(url)), Some(secrets("key")), true)
                    .unwrap()
                    .is_some()
            );
        }
    }

    #[test]
    fn websocket_url_rejects_plaintext_credentials() {
        for url in [
            "wss://user:password@pricing.test/ws",
            "wss://pricing.test/ws?token=secret",
            "wss://pricing.test/ws#token=secret",
        ] {
            assert!(matches!(
                PricingCtx::assemble(Some(config(url)), Some(secrets("key")), true),
                Err(PricingCtxError::CredentialsInWebSocketUrl)
            ));
        }
    }

    #[test]
    fn api_key_rejects_header_control_characters() {
        assert!(matches!(
            PricingCtx::assemble(
                Some(config("wss://pricing.test/ws")),
                Some(secrets("key\nsecond-header")),
                true
            ),
            Err(PricingCtxError::InvalidApiKey)
        ));
    }

    #[test]
    fn debug_output_redacts_the_api_key() {
        let ctx = PricingCtx::assemble(
            Some(config("wss://pricing.test/ws")),
            Some(secrets("pricing_raindex_super-secret")),
            true,
        )
        .unwrap()
        .unwrap();

        let rendered = format!("{ctx:?}");

        assert!(!rendered.contains("super-secret"));
        assert!(rendered.contains("<redacted>"));
    }

    #[test]
    fn gcp_id_token_without_secrets_builds_identity_auth() {
        let ctx = PricingCtx::assemble(
            Some(gcp_config("wss://st0x-pricing-x.a.run.app/ws")),
            None,
            true,
        )
        .unwrap()
        .unwrap();
        match ctx.auth {
            PricingAuth::GcpIdToken { audience } => {
                assert_eq!(audience, "https://st0x-pricing-x.a.run.app");
            }
            PricingAuth::ApiKey(_) => panic!("expected identity auth"),
        }
    }

    #[test]
    fn gcp_id_token_with_secret_is_ambiguous() {
        let err = PricingCtx::assemble(
            Some(gcp_config("wss://st0x-pricing-x.a.run.app/ws")),
            Some(PricingSecrets {
                api_key: "k".into(),
            }),
            true,
        )
        .unwrap_err();
        assert!(matches!(err, PricingCtxError::AmbiguousAuth));
    }

    #[test]
    fn gcp_id_token_requires_wss() {
        let err = PricingCtx::assemble(Some(gcp_config("ws://localhost:8080/ws")), None, true)
            .unwrap_err();
        assert!(matches!(err, PricingCtxError::IdTokenRequiresWss));
    }
}
