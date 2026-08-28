use std::fmt;

use reqwest::StatusCode;

/// Failure surfaced to the operator. `Display` explains what went wrong and,
/// for auth failures, how to fix it.
#[derive(Debug)]
pub enum Error {
    Transport(String, reqwest::Error),
    Unauthorized(String),
    Forbidden(String),
    Http(StatusCode, String),
    Decode(String),
    Encode(serde_json::Error),
    Auth(String),
}

impl Error {
    pub fn exit_code(&self) -> u8 {
        match self {
            Self::Unauthorized(_) | Self::Forbidden(_) | Self::Auth(_) => 77,
            _ => 1,
        }
    }
}

fn server_said(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        String::new()
    } else {
        format!("\nServer said: {trimmed}")
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(url, source) => {
                write!(f, "could not reach the liquidity API at {url}: {source}")
            }
            Self::Unauthorized(body) => write!(
                f,
                "HTTP 401 Unauthorized: the T0 Google identity was missing, expired, or invalid.\nRefresh the T0 login (gcloud auth login) or, in CI, the T0 workload identity, then retry.{}",
                server_said(body)
            ),
            Self::Forbidden(body) => write!(
                f,
                "HTTP 403 Forbidden: authenticated, but your T0 Workspace group is not on this command's access list.{}",
                server_said(body)
            ),
            Self::Http(status, body) => {
                write!(f, "HTTP {status}: the request failed.{}", server_said(body))
            }
            Self::Decode(detail) => write!(f, "the API response could not be decoded: {detail}"),
            Self::Encode(source) => {
                write!(f, "the response could not be re-encoded as JSON: {source}")
            }
            Self::Auth(reason) => write!(
                f,
                "could not obtain a T0 Google identity: {reason}\nFor staging, complete the browser sign-in when prompted. For production, ensure Application Default Credentials (a service account, workload identity, or impersonation) can mint an ID token for the configured audience.",
            ),
        }
    }
}

impl std::error::Error for Error {}
