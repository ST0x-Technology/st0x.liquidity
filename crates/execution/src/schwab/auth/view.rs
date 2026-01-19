use cqrs_es::{EventEnvelope, View};
use serde::{Deserialize, Serialize};

use super::super::encryption::EncryptedToken;
use super::{SchwabAuth, SchwabAuthEvent};

#[derive(Debug, thiserror::Error)]
pub(crate) enum SchwabAuthViewError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Deserialization error: {0}")]
    Deserialization(#[from] serde_json::Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SchwabAuthView {
    NotAuthenticated,
    Authenticated {
        access_token: EncryptedToken,
        access_token_fetched_at: chrono::DateTime<chrono::Utc>,
        refresh_token: EncryptedToken,
        refresh_token_fetched_at: chrono::DateTime<chrono::Utc>,
    },
}

impl Default for SchwabAuthView {
    fn default() -> Self {
        Self::NotAuthenticated
    }
}

impl View<SchwabAuth> for SchwabAuthView {
    fn update(&mut self, event: &EventEnvelope<SchwabAuth>) {
        match &event.payload {
            SchwabAuthEvent::TokensStored {
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
            } => {
                *self = Self::Authenticated {
                    access_token: access_token.clone(),
                    access_token_fetched_at: *access_token_fetched_at,
                    refresh_token: refresh_token.clone(),
                    refresh_token_fetched_at: *refresh_token_fetched_at,
                };
            }
            SchwabAuthEvent::AccessTokenRefreshed {
                access_token,
                refreshed_at,
            } => {
                if let Self::Authenticated {
                    access_token: old_access,
                    access_token_fetched_at: old_fetched_at,
                    ..
                } = self
                {
                    *old_access = access_token.clone();
                    *old_fetched_at = *refreshed_at;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schwab::encryption::{EncryptionKey, encrypt_token};
    use chrono::Utc;
    use cqrs_es::EventEnvelope;
    use std::collections::HashMap;

    fn create_test_key() -> EncryptionKey {
        "0x0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap()
    }

    #[test]
    fn test_view_update_from_tokens_stored() {
        let key = create_test_key();
        let access_token = encrypt_token(&key, "test_access").unwrap();
        let refresh_token = encrypt_token(&key, "test_refresh").unwrap();
        let now = Utc::now();

        let event = SchwabAuthEvent::TokensStored {
            access_token: access_token.clone(),
            access_token_fetched_at: now,
            refresh_token: refresh_token.clone(),
            refresh_token_fetched_at: now,
        };

        let envelope = EventEnvelope {
            aggregate_id: "schwab".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        let mut view = SchwabAuthView::default();

        assert!(matches!(view, SchwabAuthView::NotAuthenticated));

        view.update(&envelope);

        let SchwabAuthView::Authenticated {
            access_token: view_access,
            access_token_fetched_at,
            refresh_token: view_refresh,
            refresh_token_fetched_at,
        } = view
        else {
            panic!("Expected Authenticated variant");
        };

        assert_eq!(view_access, access_token);
        assert_eq!(view_refresh, refresh_token);
        assert_eq!(access_token_fetched_at, now);
        assert_eq!(refresh_token_fetched_at, now);
    }

    #[test]
    fn test_view_update_from_access_token_refreshed() {
        let key = create_test_key();
        let old_access = encrypt_token(&key, "old_access").unwrap();
        let refresh_token = encrypt_token(&key, "refresh").unwrap();
        let old_time = Utc::now();

        let mut view = SchwabAuthView::Authenticated {
            access_token: old_access,
            access_token_fetched_at: old_time,
            refresh_token: refresh_token.clone(),
            refresh_token_fetched_at: old_time,
        };

        let new_access = encrypt_token(&key, "new_access").unwrap();
        let new_time = Utc::now();

        let event = SchwabAuthEvent::AccessTokenRefreshed {
            access_token: new_access.clone(),
            refreshed_at: new_time,
        };

        let envelope = EventEnvelope {
            aggregate_id: "schwab".to_string(),
            sequence: 2,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        let SchwabAuthView::Authenticated {
            access_token,
            access_token_fetched_at,
            refresh_token: stored_refresh,
            refresh_token_fetched_at,
        } = view
        else {
            panic!("Expected Authenticated variant");
        };

        assert_eq!(access_token, new_access);
        assert_eq!(access_token_fetched_at, new_time);
        assert_eq!(stored_refresh, refresh_token);
        assert_eq!(refresh_token_fetched_at, old_time);
    }

    #[test]
    fn test_refresh_on_not_authenticated_does_not_change_state() {
        let key = create_test_key();
        let new_access = encrypt_token(&key, "new_access").unwrap();
        let new_time = Utc::now();

        let mut view = SchwabAuthView::NotAuthenticated;

        let event = SchwabAuthEvent::AccessTokenRefreshed {
            access_token: new_access,
            refreshed_at: new_time,
        };

        let envelope = EventEnvelope {
            aggregate_id: "schwab".to_string(),
            sequence: 1,
            payload: event,
            metadata: HashMap::new(),
        };

        view.update(&envelope);

        assert!(matches!(view, SchwabAuthView::NotAuthenticated));
    }
}
