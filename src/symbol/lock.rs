use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use tokio::sync::{Mutex, RwLock};

use st0x_execution::Symbol;

/// Global symbol-level locks to prevent race conditions during concurrent trade processing.
/// Each symbol gets its own mutex to ensure atomic accumulation operations.
static SYMBOL_LOCKS: LazyLock<RwLock<HashMap<String, Arc<Mutex<()>>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

/// Acquires a symbol-specific lock to ensure atomic trade processing.
/// Creates a new lock for the symbol if one doesn't exist.
pub async fn get_symbol_lock(symbol: &Symbol) -> Arc<Mutex<()>> {
    // First try to get existing lock with read lock (most common case)
    {
        let locks_read = SYMBOL_LOCKS.read().await;
        if let Some(lock) = locks_read.get(&symbol.to_string()) {
            return lock.clone();
        }
    }

    // If lock doesn't exist, acquire write lock and create new one
    let mut locks_write = SYMBOL_LOCKS.write().await;
    locks_write
        .entry(symbol.to_string())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}
