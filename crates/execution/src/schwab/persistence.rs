use async_trait::async_trait;

use super::SchwabTokens;

/// Minimal data needed for polling order status from submitted orders.
#[derive(Debug, Clone)]
pub struct SubmittedOrderRow {
    pub order_id: String,
    pub symbol: String,
    pub shares: i64,
    pub direction: String,
}

/// Trait for all persistence operations the Schwab executor needs.
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

    /// Get submitted orders for status polling.
    async fn get_submitted_orders(&self) -> Result<Vec<SubmittedOrderRow>, Self::Error>;
}

#[cfg(test)]
pub mod mock {
    use std::sync::Arc;
    use tokio::sync::RwLock;

    use super::*;

    #[derive(Debug, Clone, thiserror::Error)]
    pub enum MockPersistenceError {
        #[error("No tokens stored")]
        NoTokens,
    }

    /// In-memory mock implementation of SchwabPersistence for tests.
    #[derive(Debug, Clone)]
    pub struct MockSchwabPersistence {
        tokens: Arc<RwLock<Option<SchwabTokens>>>,
        submitted_orders: Arc<RwLock<Vec<SubmittedOrderRow>>>,
    }

    impl MockSchwabPersistence {
        pub fn new() -> Self {
            Self {
                tokens: Arc::new(RwLock::new(None)),
                submitted_orders: Arc::new(RwLock::new(Vec::new())),
            }
        }

        pub fn with_tokens(tokens: SchwabTokens) -> Self {
            Self {
                tokens: Arc::new(RwLock::new(Some(tokens))),
                submitted_orders: Arc::new(RwLock::new(Vec::new())),
            }
        }

        pub fn with_submitted_orders(orders: Vec<SubmittedOrderRow>) -> Self {
            Self {
                tokens: Arc::new(RwLock::new(None)),
                submitted_orders: Arc::new(RwLock::new(orders)),
            }
        }

        /// Set tokens directly for test setup.
        pub async fn set_tokens(&self, tokens: SchwabTokens) {
            let mut guard = self.tokens.write().await;
            *guard = Some(tokens);
        }

        /// Get stored tokens for test verification.
        pub async fn get_tokens(&self) -> Option<SchwabTokens> {
            let guard = self.tokens.read().await;
            guard.clone()
        }

        /// Set submitted orders for test setup.
        pub async fn set_submitted_orders(&self, orders: Vec<SubmittedOrderRow>) {
            let mut guard = self.submitted_orders.write().await;
            *guard = orders;
        }

        /// Get stored token count for test verification.
        pub async fn tokens_stored(&self) -> bool {
            let guard = self.tokens.read().await;
            guard.is_some()
        }
    }

    impl Default for MockSchwabPersistence {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
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

        async fn get_submitted_orders(&self) -> Result<Vec<SubmittedOrderRow>, Self::Error> {
            let guard = self.submitted_orders.read().await;
            Ok(guard.clone())
        }
    }
}
