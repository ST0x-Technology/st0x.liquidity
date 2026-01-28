use async_trait::async_trait;

use super::SchwabTokens;

/// Trait for Schwab authentication persistence.
///
/// This abstraction allows the execution crate to remain database-agnostic
/// while the main crate provides the actual SQLite implementation.
#[async_trait]
pub trait SchwabPersistence: Send + Sync + Clone + 'static {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Store tokens to persistent storage (handles encryption internally).
    async fn store_tokens(&self, tokens: &SchwabTokens) -> Result<(), Self::Error>;

    /// Load tokens from persistent storage (handles decryption internally).
    async fn load_tokens(&self) -> Result<SchwabTokens, Self::Error>;
}

#[derive(Debug, Clone, thiserror::Error)]
#[cfg(test)]
pub enum MockPersistenceError {
    #[error("No tokens stored")]
    NoTokens,
}

/// In-memory mock implementation of SchwabPersistence for tests.
#[derive(Debug, Clone)]
#[cfg(test)]
pub struct MockSchwabPersistence {
    tokens: std::sync::Arc<tokio::sync::RwLock<Option<SchwabTokens>>>,
}

#[cfg(test)]
impl MockSchwabPersistence {
    pub fn new() -> Self {
        Self {
            tokens: std::sync::Arc::new(tokio::sync::RwLock::new(None)),
        }
    }

    pub fn with_tokens(tokens: SchwabTokens) -> Self {
        Self {
            tokens: std::sync::Arc::new(tokio::sync::RwLock::new(Some(tokens))),
        }
    }

    pub async fn get_tokens(&self) -> Option<SchwabTokens> {
        let guard = self.tokens.read().await;
        guard.clone()
    }

    pub async fn tokens_stored(&self) -> bool {
        let guard = self.tokens.read().await;
        guard.is_some()
    }
}

#[cfg(test)]
impl Default for MockSchwabPersistence {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
#[cfg(test)]
impl SchwabPersistence for MockSchwabPersistence {
    type Error = MockPersistenceError;

    async fn store_tokens(&self, tokens: &SchwabTokens) -> Result<(), Self::Error> {
        let mut guard = self.tokens.write().await;
        *guard = Some(tokens.clone());
        Ok(())
    }

    async fn load_tokens(&self) -> Result<SchwabTokens, Self::Error> {
        let guard = self.tokens.read().await;
        guard.clone().ok_or(MockPersistenceError::NoTokens)
    }
}
