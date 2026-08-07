//! Shared boundary types for the issuance-bot integration.
//!
//! Every consumer of the issuance internal API (the rebalancing trigger's
//! freeze gate, mint-authorization vault-mode reads and deliveries) reduces
//! the client's errors to the classification here, at the boundary, so the
//! raw [`ClientError`] -- which embeds the secret endpoint URL in both
//! `Debug` and `Display` -- never reaches logs.

use st0x_issuance_client::ClientError;

/// Coarse, endpoint-free classification of a failed issuance request.
///
/// The issuance endpoint lives in the encrypted secrets and
/// [`ClientError`] embeds the request URL in both `Debug` and `Display`;
/// callers log outcomes, so the raw client error is reduced to this
/// classification at the boundary and never reaches logs.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IssuanceClientFailure {
    #[error("HTTP client could not be built")]
    Build,
    #[error("configured base URL is not a valid base")]
    InvalidBaseUrl,
    #[error("request timed out")]
    Timeout,
    #[error("connection failed")]
    Connect,
    #[error("transport error")]
    Transport,
    #[error("failed to parse the response body")]
    ParseResponse,
    /// Raw status code: the client does not re-export its `StatusCode` type,
    /// and naming either of the workspace's two reqwest majors here would tie
    /// this enum to the client's private dependency version.
    #[error("unexpected status {0}")]
    UnexpectedStatus(u16),
}

impl From<ClientError> for IssuanceClientFailure {
    fn from(error: ClientError) -> Self {
        match error {
            ClientError::Build(_) => Self::Build,
            ClientError::NotABase { .. } => Self::InvalidBaseUrl,
            ClientError::Http(http) if http.is_timeout() => Self::Timeout,
            ClientError::Http(http) if http.is_connect() => Self::Connect,
            ClientError::Http(_) => Self::Transport,
            ClientError::ParseResponse(_) => Self::ParseResponse,
            ClientError::Status { status, .. } => Self::UnexpectedStatus(status.as_u16()),
        }
    }
}
