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

#[cfg(test)]
mod tests {
    use alloy::primitives::b256;

    use super::*;

    #[tokio::test]
    async fn new_cache_is_empty() {
        let cache = FeedIdCache::new();

        assert!(cache.get("AAPL").await.is_none());
    }

    #[tokio::test]
    async fn insert_then_get_returns_feed_id() {
        let cache = FeedIdCache::new();
        let feed_id = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        cache.insert("AAPL".to_string(), feed_id).await;

        assert_eq!(cache.get("AAPL").await, Some(feed_id));
    }

    #[tokio::test]
    async fn get_unknown_symbol_returns_none() {
        let cache = FeedIdCache::new();
        let feed_id = b256!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        cache.insert("AAPL".to_string(), feed_id).await;

        assert!(cache.get("TSLA").await.is_none());
    }

    #[tokio::test]
    async fn insert_overwrites_previous_value() {
        let cache = FeedIdCache::new();
        let first = b256!("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        let second = b256!("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");

        cache.insert("AAPL".to_string(), first).await;
        cache.insert("AAPL".to_string(), second).await;

        assert_eq!(cache.get("AAPL").await, Some(second));
    }

    #[tokio::test]
    async fn clone_shares_state() {
        let cache = FeedIdCache::new();
        let cloned = cache.clone();
        let feed_id = b256!("0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc");

        cache.insert("AAPL".to_string(), feed_id).await;

        assert_eq!(cloned.get("AAPL").await, Some(feed_id));
    }
}
