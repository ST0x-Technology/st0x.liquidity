use alloy::primitives::{B256, fixed_bytes};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct FeedIdCache {
    cache: Arc<RwLock<HashMap<String, B256>>>,
}

impl FeedIdCache {
    pub fn new() -> Self {
        let initial = HashMap::from([
            (
                "AAPL".to_string(),
                fixed_bytes!("0x49f6b65cb1de6b10eaf75e7c03ca029c306d0357e91b5311b175084a5ad55688"),
            ),
            (
                "MSFT".to_string(),
                fixed_bytes!("0xd0ca23c1cc005e004ccf1db5bf76aeb6a49218f43dac3d4b275e92de12ded4d1"),
            ),
            (
                "GOOG".to_string(),
                fixed_bytes!("0xe65ff435be42630439c96396653a342829e877e2aafaeaf1a10d0ee5fd2cf3f2"),
            ),
            (
                "AMZN".to_string(),
                fixed_bytes!("0xb5d0e0fa58a1f8b81498ae670ce93c872d14434b72c364885d4fa1b257cbb07a"),
            ),
            (
                "NVDA".to_string(),
                fixed_bytes!("0xb1073854ed24cbc755dc527418f52b7d271f6cc967bbf8d8129112b18860a593"),
            ),
            (
                "META".to_string(),
                fixed_bytes!("0x78a3e3b8e676a8f73c439f5d749737034b139bbbe899ba5775216fba596607fe"),
            ),
            (
                "TSLA".to_string(),
                fixed_bytes!("0x16dad506d7db8da01c87581c87ca897a012a153557d4d578c3b9c9e1bc0632f1"),
            ),
            (
                "GME".to_string(),
                fixed_bytes!("0x6f9cd89ef1b7fd39f667101a91ad578b6c6ace4579d5f7f285a4b06aa4504be6"),
            ),
        ]);

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
