use std::time::{Duration, Instant};

use dashmap::DashMap;
use once_cell::sync::Lazy;

use crate::config::vars;

/// Cache entry for parsed SELECT queries.
struct CacheEntry {
    expiration: Instant,
    query: String,
}

/// Global SELECT cache — DashMap for concurrent access.
static CACHE: Lazy<DashMap<String, CacheEntry>> = Lazy::new(DashMap::new);

/// Cache TTL: 10 seconds (matching Go implementation).
const CACHE_TTL: Duration = Duration::from_secs(10);

/// Enable the SELECT cache and start the cleanup daemon.
pub fn enable_select_cache() {
    vars::set_select_cache_enabled(true);
    tokio::spawn(cache_cleanup());
}

/// Disable the SELECT cache and clear all entries.
pub fn disable_select_cache() {
    vars::set_select_cache_enabled(false);
    CACHE.clear();
}

/// Check if a query is in the cache.
pub fn get_cached_query(query: &str) -> Option<String> {
    if !vars::select_cache_enabled() {
        return None;
    }

    if let Some(entry) = CACHE.get(query) {
        if Instant::now() < entry.expiration {
            return Some(entry.query.clone());
        }
    }
    None
}

/// Store a query result in the cache.
pub fn set_cached_query(query: &str) {
    if !vars::select_cache_enabled() {
        return;
    }

    CACHE.insert(
        query.to_string(),
        CacheEntry {
            expiration: Instant::now() + CACHE_TTL,
            query: query.to_string(),
        },
    );
}

/// Background cleanup: remove expired entries every second.
async fn cache_cleanup() {
    loop {
        if !vars::select_cache_enabled() {
            break;
        }

        let now = Instant::now();
        CACHE.retain(|_, entry| now < entry.expiration);

        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}
