use anyhow::{Context, Result};
use url::Url;

/// Deployment the client talks to. Selects the IAP-fronted API base URL and
/// the per-role ID token audiences.
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
/// IAP-fronted HTTPS endpoint; the two audiences name the read and write
/// backend services the ID tokens are minted for. None of these is a
/// credential - each audience is worthless without an identity Google signs.
pub struct Target {
    pub base_url: Url,
    pub read_audience: String,
    pub write_audience: String,
    /// Optional T0 Cloud Logging console URL, printed alongside API errors.
    pub logging_url: Option<String>,
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
    let read_audience = required(
        &format!("{prefix}_READ_AUDIENCE"),
        "to the read backend audience (the terraform ops_api_audiences read value)",
    )?;
    let write_audience = required(
        &format!("{prefix}_WRITE_AUDIENCE"),
        "to the write backend audience (the terraform ops_api_audiences write value)",
    )?;
    let logging_url = std::env::var(format!("{prefix}_LOGGING_URL")).ok();
    Ok(Target {
        base_url,
        read_audience,
        write_audience,
        logging_url,
    })
}
