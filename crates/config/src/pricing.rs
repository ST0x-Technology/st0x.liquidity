//! Pricing-service configuration and secret assembly.

use serde::Deserialize;
use url::{Host, Url};

/// Non-secret pricing-service transport settings.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    pub ws_url: Url,
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

/// Runtime context for the dashboard's pricing-service subscription.
#[derive(Clone, Debug)]
pub struct PricingCtx {
    pub ws_url: Url,
    pub api_key: PricingApiKey,
}

impl PricingCtx {
    /// Builds a pricing context from explicit runtime inputs.
    pub fn new(ws_url: Url, api_key: String) -> Result<Self, PricingCtxError> {
        Self::assemble(
            Some(PricingConfig { ws_url }),
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
        match (config, secrets) {
            (Some(config), Some(secrets)) => {
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

                if secrets.api_key.trim().is_empty() {
                    return Err(PricingCtxError::EmptyApiKey);
                }
                if !secrets.api_key.bytes().all(|byte| byte.is_ascii_graphic()) {
                    return Err(PricingCtxError::InvalidApiKey);
                }

                Ok(Some(Self {
                    ws_url: config.ws_url,
                    api_key: PricingApiKey(secrets.api_key),
                }))
            }
            (None, None) if !required => Ok(None),
            (None, _) => Err(PricingCtxError::MissingConfig),
            (_, None) => Err(PricingCtxError::MissingSecrets),
        }
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(url: &str) -> PricingConfig {
        PricingConfig {
            ws_url: Url::parse(url).unwrap(),
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
}
