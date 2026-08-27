use anyhow::{Context, Result};
use url::Url;

/// Deployment the client talks to. Selects the IAP-fronted API base URL and
/// the ID token audience.
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

/// Non-secret connection settings for one environment. The base URL is the
/// IAP-fronted HTTPS endpoint; the audience is the IAP OAuth client ID the ID
/// token is minted for. Neither is a credential.
pub struct Target {
    pub base_url: Url,
    pub audience: String,
}

fn required(variable: &str, hint: &str) -> Result<String> {
    std::env::var(variable).with_context(|| format!("set {variable} {hint}"))
}

pub fn resolve(env: Env) -> Result<Target> {
    let prefix = env.prefix();
    let raw_url = required(
        &format!("{prefix}_URL"),
        "to the IAP-fronted liquidity API base URL for this environment",
    )?;
    let base_url = Url::parse(&raw_url)
        .with_context(|| format!("{prefix}_URL is not a valid URL: {raw_url}"))?;
    let audience = required(
        &format!("{prefix}_AUDIENCE"),
        "to the IAP OAuth client ID (the ID token audience) for this environment",
    )?;
    Ok(Target { base_url, audience })
}
