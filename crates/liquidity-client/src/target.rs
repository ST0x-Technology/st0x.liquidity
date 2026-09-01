//! Target resolution: reads each environment's base URL and auth inputs from
//! the process environment and validates them into a `Target`.

use std::time::Duration;

use anyhow::{Context, Result};
use url::Url;

/// Deployment the client talks to. Selects the IAP-fronted API base URL and the
/// environment's auth model.
#[derive(Clone, Copy, clap::ValueEnum)]
pub enum Env {
    Staging,
    Production,
}

impl Env {
    fn prefix(self) -> &'static str {
        match self {
            Self::Staging => "T0_LIQUIDITY_STAGING",
            Self::Production => "T0_LIQUIDITY_PROD",
        }
    }
}

/// How the client authenticates to IAP for one environment.
///
/// Staging uses a Google Desktop OAuth client: an interactive browser sign-in
/// mints one ID token whose `aud` is the OAuth client id, and IAP admits both
/// role prefixes on that identity, gating read vs write by Workspace group.
/// Production uses Application Default Credentials to mint a per-role,
/// audience-bound ID token (a service account or workload identity), the only
/// non-interactive path.
pub enum Auth {
    OauthDesktop {
        client_id: String,
        client_secret: String,
    },
    Adc {
        read_audience: String,
        write_audience: String,
    },
}

/// Non-secret connection settings for one environment. The base URL is the
/// IAP-fronted HTTPS endpoint; `auth` names the identity inputs, none of which
/// is a credential on its own - each is worthless without an identity Google
/// signs.
pub struct Target {
    pub base_url: Url,
    pub auth: Auth,
    /// Optional T0 Cloud Logging console URL, printed alongside API errors.
    pub logging_url: Option<String>,
    /// Overall per-request timeout for the HTTP client.
    pub request_timeout: Duration,
    /// TCP connect timeout for the HTTP client.
    pub connect_timeout: Duration,
}

fn required(variable: &str, hint: &str) -> Result<String> {
    std::env::var(variable).with_context(|| format!("set {variable} {hint}"))
}

/// Parses and validates the API base URL: it must be a well-formed HTTPS URL
/// with a host, since the client only talks to the IAP-fronted HTTPS endpoint.
fn parse_base_url(prefix: &str, raw: &str) -> Result<Url> {
    let url = Url::parse(raw).with_context(|| format!("{prefix}_URL is not a valid URL: {raw}"))?;
    if url.scheme() != "https" {
        anyhow::bail!("{prefix}_URL must use https, got: {raw}");
    }
    if url.host_str().unwrap_or_default().is_empty() {
        anyhow::bail!("{prefix}_URL must include a host, got: {raw}");
    }
    Ok(url)
}

/// Parses a positive integer number of seconds into a `Duration`, rejecting
/// zero and non-numeric values so a misconfigured timeout fails loudly.
fn parse_timeout_secs(name: &str, raw: &str) -> Result<Duration> {
    let secs: u64 = raw
        .parse()
        .with_context(|| format!("{name} must be a whole number of seconds, got: {raw}"))?;
    if secs == 0 {
        anyhow::bail!("{name} must be greater than zero");
    }
    Ok(Duration::from_secs(secs))
}

/// Reads a required timeout setting from the environment and parses it.
fn required_timeout(prefix: &str, suffix: &str, hint: &str) -> Result<Duration> {
    let name = format!("{prefix}_{suffix}");
    let raw = required(&name, hint)?;
    parse_timeout_secs(&name, &raw)
}

pub fn resolve(env: Env) -> Result<Target> {
    let prefix = env.prefix();
    let raw_url = required(
        &format!("{prefix}_URL"),
        "to the IAP-fronted liquidity API base URL for this environment",
    )?;
    let base_url = parse_base_url(prefix, &raw_url)?;
    let auth = match env {
        Env::Staging => Auth::OauthDesktop {
            client_id: required(
                &format!("{prefix}_CLIENT_ID"),
                "to the desktop OAuth client id for staging",
            )?,
            client_secret: required(
                &format!("{prefix}_CLIENT_SECRET"),
                "to the desktop OAuth client secret (it ships in the CLI; the browser sign-in is the security)",
            )?,
        },
        Env::Production => Auth::Adc {
            read_audience: required(
                &format!("{prefix}_READ_AUDIENCE"),
                "to the read backend audience (the terraform ops_api_audiences read value)",
            )?,
            write_audience: required(
                &format!("{prefix}_WRITE_AUDIENCE"),
                "to the write backend audience (the terraform ops_api_audiences write value)",
            )?,
        },
    };
    let logging_url = std::env::var(format!("{prefix}_LOGGING_URL")).ok();
    let request_timeout = required_timeout(
        prefix,
        "REQUEST_TIMEOUT_SECS",
        "to the overall per-request timeout in seconds",
    )?;
    let connect_timeout = required_timeout(
        prefix,
        "CONNECT_TIMEOUT_SECS",
        "to the TCP connect timeout in seconds",
    )?;
    Ok(Target {
        base_url,
        auth,
        logging_url,
        request_timeout,
        connect_timeout,
    })
}

#[cfg(test)]
mod tests {
    //! Tests for URL and timeout validation and target resolution.
    use serial_test::serial;

    use super::{Env, parse_base_url, parse_timeout_secs, resolve};

    #[test]
    fn accepts_https_url_with_host() {
        assert_eq!(
            parse_base_url("T0_LIQUIDITY_STAGING", "https://liquidity.example.com")
                .ok()
                .map(|url| url.to_string()),
            Some("https://liquidity.example.com/".to_owned())
        );
    }

    #[test]
    fn rejects_non_https_scheme() {
        assert!(parse_base_url("T0_LIQUIDITY_STAGING", "http://liquidity.example.com").is_err());
    }

    #[test]
    fn rejects_url_without_host() {
        assert!(parse_base_url("T0_LIQUIDITY_STAGING", "https://").is_err());
    }

    #[test]
    fn rejects_malformed_url() {
        assert!(parse_base_url("T0_LIQUIDITY_STAGING", "not a url").is_err());
    }

    #[test]
    fn parses_positive_timeout_seconds() {
        assert_eq!(
            parse_timeout_secs("T", "30").ok(),
            Some(std::time::Duration::from_secs(30))
        );
    }

    #[test]
    fn rejects_zero_timeout() {
        assert!(parse_timeout_secs("T", "0").is_err());
    }

    #[test]
    fn rejects_non_numeric_timeout() {
        assert!(parse_timeout_secs("T", "abc").is_err());
    }

    #[test]
    #[serial]
    fn resolve_fails_when_staging_request_timeout_is_missing() {
        temp_env::with_vars(
            [
                ("T0_LIQUIDITY_STAGING_URL", Some("https://x.example.com")),
                ("T0_LIQUIDITY_STAGING_CLIENT_ID", Some("cid")),
                ("T0_LIQUIDITY_STAGING_CLIENT_SECRET", Some("secret")),
                ("T0_LIQUIDITY_STAGING_REQUEST_TIMEOUT_SECS", None),
                ("T0_LIQUIDITY_STAGING_CONNECT_TIMEOUT_SECS", Some("10")),
            ],
            || assert!(resolve(Env::Staging).is_err()),
        );
    }

    #[test]
    #[serial]
    fn resolve_fails_when_staging_connect_timeout_is_missing() {
        temp_env::with_vars(
            [
                ("T0_LIQUIDITY_STAGING_URL", Some("https://x.example.com")),
                ("T0_LIQUIDITY_STAGING_CLIENT_ID", Some("cid")),
                ("T0_LIQUIDITY_STAGING_CLIENT_SECRET", Some("secret")),
                ("T0_LIQUIDITY_STAGING_REQUEST_TIMEOUT_SECS", Some("30")),
                ("T0_LIQUIDITY_STAGING_CONNECT_TIMEOUT_SECS", None),
            ],
            || assert!(resolve(Env::Staging).is_err()),
        );
    }

    #[test]
    #[serial]
    fn resolve_fails_when_production_request_timeout_is_missing() {
        temp_env::with_vars(
            [
                ("T0_LIQUIDITY_PROD_URL", Some("https://x.example.com")),
                ("T0_LIQUIDITY_PROD_READ_AUDIENCE", Some("read")),
                ("T0_LIQUIDITY_PROD_WRITE_AUDIENCE", Some("write")),
                ("T0_LIQUIDITY_PROD_REQUEST_TIMEOUT_SECS", None),
                ("T0_LIQUIDITY_PROD_CONNECT_TIMEOUT_SECS", Some("10")),
            ],
            || assert!(resolve(Env::Production).is_err()),
        );
    }

    #[test]
    #[serial]
    fn resolve_fails_when_production_connect_timeout_is_missing() {
        temp_env::with_vars(
            [
                ("T0_LIQUIDITY_PROD_URL", Some("https://x.example.com")),
                ("T0_LIQUIDITY_PROD_READ_AUDIENCE", Some("read")),
                ("T0_LIQUIDITY_PROD_WRITE_AUDIENCE", Some("write")),
                ("T0_LIQUIDITY_PROD_REQUEST_TIMEOUT_SECS", Some("30")),
                ("T0_LIQUIDITY_PROD_CONNECT_TIMEOUT_SECS", None),
            ],
            || assert!(resolve(Env::Production).is_err()),
        );
    }
}
