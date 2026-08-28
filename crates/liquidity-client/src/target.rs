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
    Ok(Target {
        base_url,
        auth,
        logging_url,
    })
}
