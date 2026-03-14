//! Thread-safe cache mapping token symbols to Pyth oracle feed IDs.

use alloy::primitives::B256;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct FeedIdCache {
    cache: Arc<RwLock<HashMap<String, B256>>>,
}

impl FeedIdCache {
    pub fn new() -> Self {
        let initial = HashMap::new();

        Self {
            cache: Arc::new(RwLock::new(initial)),
        }
    }

    pub async fn get(&self, symbol: &str) -> Option<B256> {
        self.cache.read().await.get(symbol).copied()
    }

    pub async fn insert(&self, symbol: String, feed_id: B256) {
        let mut cache = self.cache.write().await;
        cache.insert(symbol, feed_id);
    }
}

impl Default for FeedIdCache {
    fn default() -> Self {
        Self::new()
    }
}
