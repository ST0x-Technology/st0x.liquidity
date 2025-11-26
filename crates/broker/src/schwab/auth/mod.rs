use async_trait::async_trait;
use chrono::Utc;
use cqrs_es::Aggregate;
use serde::{Deserialize, Serialize};

use super::encryption::{EncryptedToken, EncryptionError, EncryptionKey, encrypt_token};

mod cmd;
mod event;
mod oauth;
pub(crate) mod view;

pub use cmd::{AccessToken, RefreshToken, SchwabAuthCommand};
pub use event::SchwabAuthEvent;
pub use oauth::SchwabAuthEnv;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SchwabAuth {
    #[default]
    NotAuthenticated,
    Authenticated {
        access_token: EncryptedToken,
        access_token_fetched_at: chrono::DateTime<Utc>,
        refresh_token: EncryptedToken,
        refresh_token_fetched_at: chrono::DateTime<Utc>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum SchwabAuthError {
    #[error("Cannot refresh access token: not authenticated")]
    NotAuthenticated,
    #[error("Encryption failed: {0}")]
    Encryption(#[from] EncryptionError),
}

#[async_trait]
impl Aggregate for SchwabAuth {
    type Command = SchwabAuthCommand;
    type Event = SchwabAuthEvent;
    type Error = SchwabAuthError;
    type Services = EncryptionKey;

    fn aggregate_type() -> String {
        "SchwabAuth".to_string()
    }

    async fn handle(
        &self,
        command: Self::Command,
        encryption_key: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            SchwabAuthCommand::Migrate {
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
            } => Ok(vec![SchwabAuthEvent::TokensStored {
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
            }]),
            SchwabAuthCommand::StoreTokens {
                access_token,
                refresh_token,
            } => {
                let now = Utc::now();

                let encrypted_access = encrypt_token(encryption_key, access_token.as_str())?;
                let encrypted_refresh = encrypt_token(encryption_key, refresh_token.as_str())?;

                Ok(vec![SchwabAuthEvent::TokensStored {
                    access_token: encrypted_access,
                    access_token_fetched_at: now,
                    refresh_token: encrypted_refresh,
                    refresh_token_fetched_at: now,
                }])
            }
            SchwabAuthCommand::RefreshAccessToken { new_access_token } => {
                if matches!(self, Self::NotAuthenticated) {
                    return Err(SchwabAuthError::NotAuthenticated);
                }

                let now = Utc::now();

                let encrypted_access = encrypt_token(encryption_key, new_access_token.as_str())?;

                Ok(vec![SchwabAuthEvent::AccessTokenRefreshed {
                    access_token: encrypted_access,
                    refreshed_at: now,
                }])
            }
        }
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            SchwabAuthEvent::TokensStored {
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
            } => {
                *self = Self::Authenticated {
                    access_token,
                    access_token_fetched_at,
                    refresh_token,
                    refresh_token_fetched_at,
                };
            }
            SchwabAuthEvent::AccessTokenRefreshed {
                access_token,
                refreshed_at,
            } => {
                if let Self::Authenticated {
                    access_token: old_access,
                    access_token_fetched_at: old_fetched_at,
                    refresh_token: _,
                    refresh_token_fetched_at: _,
                } = self
                {
                    *old_access = access_token;
                    *old_fetched_at = refreshed_at;
                } else {
                    *self = Self::NotAuthenticated;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_key() -> EncryptionKey {
        "0x0000000000000000000000000000000000000000000000000000000000000000"
            .parse()
            .unwrap()
    }

    #[tokio::test]
    async fn test_store_tokens_from_not_authenticated() {
        let aggregate = SchwabAuth::NotAuthenticated;
        let encryption_key = create_test_key();
        let command = SchwabAuthCommand::StoreTokens {
            access_token: cmd::AccessToken::new("test_access_token".to_string()),
            refresh_token: cmd::RefreshToken::new("test_refresh_token".to_string()),
        };

        let events = aggregate.handle(command, &encryption_key).await.unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            SchwabAuthEvent::TokensStored {
                access_token,
                access_token_fetched_at,
                refresh_token,
                refresh_token_fetched_at,
            } => {
                assert!(!access_token.as_bytes().is_empty());
                assert!(!refresh_token.as_bytes().is_empty());
                assert!(
                    Utc::now()
                        .signed_duration_since(*access_token_fetched_at)
                        .num_seconds()
                        < 2
                );
                assert!(
                    Utc::now()
                        .signed_duration_since(*refresh_token_fetched_at)
                        .num_seconds()
                        < 2
                );
            }
            SchwabAuthEvent::AccessTokenRefreshed { .. } => panic!("Expected TokensStored event"),
        }
    }

    #[test]
    fn test_apply_tokens_stored() {
        let mut aggregate = SchwabAuth::NotAuthenticated;
        let encryption_key = create_test_key();

        let access_token = encrypt_token(&encryption_key, "test_access").unwrap();
        let refresh_token = encrypt_token(&encryption_key, "test_refresh").unwrap();
        let now = Utc::now();

        let event = SchwabAuthEvent::TokensStored {
            access_token: access_token.clone(),
            access_token_fetched_at: now,
            refresh_token: refresh_token.clone(),
            refresh_token_fetched_at: now,
        };

        aggregate.apply(event);

        match aggregate {
            SchwabAuth::Authenticated {
                access_token: stored_access,
                access_token_fetched_at,
                refresh_token: stored_refresh,
                refresh_token_fetched_at,
            } => {
                assert_eq!(stored_access, access_token);
                assert_eq!(stored_refresh, refresh_token);
                assert_eq!(access_token_fetched_at, now);
                assert_eq!(refresh_token_fetched_at, now);
            }
            SchwabAuth::NotAuthenticated => panic!("Expected Authenticated state"),
        }
    }

    #[tokio::test]
    async fn test_refresh_access_token_when_authenticated() {
        let encryption_key = create_test_key();
        let access_token = encrypt_token(&encryption_key, "old_access").unwrap();
        let refresh_token = encrypt_token(&encryption_key, "refresh").unwrap();
        let now = Utc::now();

        let aggregate = SchwabAuth::Authenticated {
            access_token,
            access_token_fetched_at: now,
            refresh_token,
            refresh_token_fetched_at: now,
        };

        let command = SchwabAuthCommand::RefreshAccessToken {
            new_access_token: cmd::AccessToken::new("new_access".to_string()),
        };

        let events = aggregate.handle(command, &encryption_key).await.unwrap();
        assert_eq!(events.len(), 1);

        match &events[0] {
            SchwabAuthEvent::AccessTokenRefreshed {
                access_token,
                refreshed_at,
            } => {
                assert!(!access_token.as_bytes().is_empty());
                assert!(
                    Utc::now()
                        .signed_duration_since(*refreshed_at)
                        .num_seconds()
                        < 2
                );
            }
            SchwabAuthEvent::TokensStored { .. } => panic!("Expected AccessTokenRefreshed event"),
        }
    }

    #[tokio::test]
    async fn test_refresh_access_token_when_not_authenticated() {
        let aggregate = SchwabAuth::NotAuthenticated;
        let encryption_key = create_test_key();
        let command = SchwabAuthCommand::RefreshAccessToken {
            new_access_token: cmd::AccessToken::new("new_access".to_string()),
        };

        let result = aggregate.handle(command, &encryption_key).await;
        assert!(matches!(
            result.unwrap_err(),
            SchwabAuthError::NotAuthenticated
        ));
    }

    #[test]
    fn test_apply_access_token_refreshed() {
        let encryption_key = create_test_key();
        let old_access = encrypt_token(&encryption_key, "old_access").unwrap();
        let refresh_token = encrypt_token(&encryption_key, "refresh").unwrap();
        let old_time = Utc::now();

        let mut aggregate = SchwabAuth::Authenticated {
            access_token: old_access,
            access_token_fetched_at: old_time,
            refresh_token: refresh_token.clone(),
            refresh_token_fetched_at: old_time,
        };

        let new_access = encrypt_token(&encryption_key, "new_access").unwrap();
        let new_time = Utc::now();

        let event = SchwabAuthEvent::AccessTokenRefreshed {
            access_token: new_access.clone(),
            refreshed_at: new_time,
        };

        aggregate.apply(event);

        match aggregate {
            SchwabAuth::Authenticated {
                access_token,
                access_token_fetched_at,
                refresh_token: stored_refresh,
                refresh_token_fetched_at,
            } => {
                assert_eq!(access_token, new_access);
                assert_eq!(access_token_fetched_at, new_time);
                assert_eq!(stored_refresh, refresh_token);
                assert_eq!(refresh_token_fetched_at, old_time);
            }
            SchwabAuth::NotAuthenticated => panic!("Expected Authenticated state"),
        }
    }

    #[test]
    fn test_aggregate_type() {
        assert_eq!(SchwabAuth::aggregate_type(), "SchwabAuth");
    }
}
