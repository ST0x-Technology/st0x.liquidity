//! RatioCache for deduplicating burst calls to vault ratio lookups.
//!
//! The cache uses a 2-second TTL which is short enough to stay fresh
//! (ratio only changes on vault deposit/withdraw transactions) while
//! deduplicating burst calls that occur during rapid event processing.

use std::collections::HashMap;
use std::sync::RwLock;
use std::time::{Duration, Instant};

use alloy::primitives::Address;

use super::ratio::VaultRatio;

/// Time-to-live for cached ratios.
const CACHE_TTL: Duration = Duration::from_secs(2);

/// Entry in the ratio cache with expiration time.
#[derive(Debug, Clone)]
struct CacheEntry {
    ratio: VaultRatio,
    expires_at: Instant,
}

impl CacheEntry {
    fn new(ratio: VaultRatio) -> Self {
        Self {
            ratio,
            expires_at: Instant::now() + CACHE_TTL,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() >= self.expires_at
    }
}

/// Cache for vault ratios with 2-second TTL.
///
/// Thread-safe cache that stores conversion ratios for wrapped tokens.
/// The short TTL ensures ratios stay fresh while preventing redundant
/// RPC calls during burst activity (e.g., processing multiple trades).
#[derive(Debug, Default)]
pub(crate) struct RatioCache {
    entries: RwLock<HashMap<Address, CacheEntry>>,
}

impl RatioCache {
    /// Creates a new empty cache.
    pub(crate) fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Gets a cached ratio if it exists and hasn't expired.
    pub(crate) fn get(&self, wrapped_token: &Address) -> Option<VaultRatio> {
        let entries = match self.entries.read() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };

        entries.get(wrapped_token).and_then(|entry| {
            if entry.is_expired() {
                None
            } else {
                Some(entry.ratio)
            }
        })
    }

    /// Stores a ratio in the cache with the default TTL.
    pub(crate) fn set(&self, wrapped_token: Address, ratio: VaultRatio) {
        let mut entries = match self.entries.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };

        entries.insert(wrapped_token, CacheEntry::new(ratio));
    }

    /// Removes a ratio from the cache.
    ///
    /// Used when we know the ratio has changed (e.g., after a wrap/unwrap).
    pub(crate) fn invalidate(&self, wrapped_token: &Address) {
        let mut entries = match self.entries.write() {
            Ok(guard) => guard,
            Err(poison) => poison.into_inner(),
        };

        entries.remove(wrapped_token);
    }
}

#[cfg(test)]
mod tests {
    use alloy::primitives::address;

    use super::*;

    #[test]
    fn cache_stores_and_retrieves_ratio() {
        let cache = RatioCache::new();
        let token = address!("1234567890123456789012345678901234567890");
        let ratio = VaultRatio::one_to_one();

        cache.set(token, ratio);

        let retrieved = cache.get(&token);
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap(), ratio);
    }

    #[test]
    fn cache_returns_none_for_unknown_token() {
        let cache = RatioCache::new();
        let token = address!("1234567890123456789012345678901234567890");

        let retrieved = cache.get(&token);
        assert!(retrieved.is_none());
    }

    #[test]
    fn invalidate_removes_entry() {
        let cache = RatioCache::new();
        let token = address!("1234567890123456789012345678901234567890");
        let ratio = VaultRatio::one_to_one();

        cache.set(token, ratio);
        assert!(cache.get(&token).is_some());

        cache.invalidate(&token);
        assert!(cache.get(&token).is_none());
    }

    #[test]
    fn expired_entry_returns_none() {
        let cache = RatioCache::new();
        let token = address!("1234567890123456789012345678901234567890");
        let ratio = VaultRatio::one_to_one();

        cache.set(token, ratio);

        // Force expiration
        {
            let mut entries = cache.entries.write().unwrap();
            if let Some(entry) = entries.get_mut(&token) {
                entry.expires_at = Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .expect("test always runs after process start");
            }
        }

        assert!(cache.get(&token).is_none());
    }
}
