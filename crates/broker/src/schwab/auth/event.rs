use chrono::{DateTime, Utc};
use cqrs_es::DomainEvent;
use serde::{Deserialize, Serialize};

use super::super::encryption::EncryptedToken;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchwabAuthEvent {
    TokensStored {
        access_token: EncryptedToken,
        access_token_fetched_at: DateTime<Utc>,
        refresh_token: EncryptedToken,
        refresh_token_fetched_at: DateTime<Utc>,
    },
    AccessTokenRefreshed {
        access_token: EncryptedToken,
        refreshed_at: DateTime<Utc>,
    },
}

impl DomainEvent for SchwabAuthEvent {
    fn event_type(&self) -> String {
        match self {
            Self::TokensStored { .. } => "TokensStored".to_string(),
            Self::AccessTokenRefreshed { .. } => "AccessTokenRefreshed".to_string(),
        }
    }

    fn event_version(&self) -> String {
        "1.0".to_string()
    }
}
