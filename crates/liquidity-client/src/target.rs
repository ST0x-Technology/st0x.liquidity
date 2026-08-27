use anyhow::{Context, Result};
use url::Url;

/// Deployment the client talks to. Selects the IAP-fronted API base URL.
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

/// Reads the base URL for `env` from its non-secret environment variable.
/// The value is the IAP-fronted HTTPS URL Google authenticates in front of;
/// it carries no credentials.
pub fn resolve(env: Env) -> Result<Url> {
    let prefix = env.prefix();
    let variable = format!("{prefix}_URL");
    let raw = std::env::var(&variable).with_context(|| {
        format!("set {variable} to the IAP-fronted liquidity API base URL for this environment")
    })?;
    Url::parse(&raw).with_context(|| format!("{variable} is not a valid URL: {raw}"))
}
