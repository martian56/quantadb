use crate::page::PageId;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// A thread-safe, byte-bounded LRU over immutable page-derived values.
///
/// QuantaDB never rewrites a page that a decoded value came from, so entries
/// can only become garbage, never stale. The single lock is cheap next to
/// the store mutex and page decode this cache exists to skip.
pub struct SharedByteLru<V> {
    inner: Mutex<LruInner<V>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ByteCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: usize,
}

struct LruEntry<V> {
    value: Arc<V>,
    bytes: usize,
    tick: u64,
}

struct LruInner<V> {
    map: HashMap<PageId, LruEntry<V>>,
    recency: BTreeMap<u64, PageId>,
    next_tick: u64,
    bytes: usize,
    capacity: usize,
    hits: u64,
    misses: u64,
}

impl<V> SharedByteLru<V> {
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(LruInner {
                map: HashMap::new(),
                recency: BTreeMap::new(),
                next_tick: 0,
                bytes: 0,
                capacity: capacity_bytes,
                hits: 0,
                misses: 0,
            }),
        }
    }

    #[must_use]
    pub fn stats(&self) -> ByteCacheStats {
        self.inner.lock().map_or_else(
            |_| ByteCacheStats::default(),
            |inner| ByteCacheStats {
                hits: inner.hits,
                misses: inner.misses,
                entries: inner.map.len(),
                bytes: inner.bytes,
            },
        )
    }

    pub fn get(&self, page_id: PageId) -> Option<Arc<V>> {
        let mut inner = self.inner.lock().ok()?;
        let tick = inner.take_tick();
        if let Some(entry) = inner.map.get_mut(&page_id) {
            let value = Arc::clone(&entry.value);
            let previous = std::mem::replace(&mut entry.tick, tick);
            inner.recency.remove(&previous);
            inner.recency.insert(tick, page_id);
            inner.hits += 1;
            Some(value)
        } else {
            inner.misses += 1;
            None
        }
    }

    /// Insert a decoded value, accounting `bytes` against the budget.
    ///
    /// Values larger than the whole budget are not cached at all.
    pub fn insert(&self, page_id: PageId, value: &Arc<V>, bytes: usize) {
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if bytes > inner.capacity {
            return;
        }
        let tick = inner.take_tick();
        if let Some(previous) = inner.map.insert(
            page_id,
            LruEntry {
                value: Arc::clone(value),
                bytes,
                tick,
            },
        ) {
            inner.recency.remove(&previous.tick);
            inner.bytes -= previous.bytes;
        }
        inner.recency.insert(tick, page_id);
        inner.bytes += bytes;

        while inner.bytes > inner.capacity {
            let Some((_, oldest)) = inner.recency.pop_first() else {
                break;
            };
            if let Some(evicted) = inner.map.remove(&oldest) {
                inner.bytes -= evicted.bytes;
            }
        }
    }
}

impl<V> LruInner<V> {
    fn take_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick += 1;
        tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn value(text: &str) -> Arc<String> {
        Arc::new(text.to_owned())
    }

    #[test]
    fn serves_hits_and_counts_misses() {
        let cache = SharedByteLru::new(1 << 20);
        assert!(cache.get(PageId(7)).is_none());
        cache.insert(PageId(7), &value("payload"), 64);
        assert_eq!(cache.get(PageId(7)).as_deref(), Some(&"payload".to_owned()));
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bytes, 64);
    }

    #[test]
    fn evicts_least_recently_used_within_budget() {
        let cache = SharedByteLru::new(128);
        cache.insert(PageId(1), &value("one"), 64);
        cache.insert(PageId(2), &value("two"), 64);
        assert!(cache.get(PageId(1)).is_some(), "page 1 must be hot now");
        cache.insert(PageId(3), &value("three"), 64);

        assert!(cache.get(PageId(1)).is_some(), "recently used survives");
        assert!(cache.get(PageId(2)).is_none(), "least recent is evicted");
        assert!(cache.get(PageId(3)).is_some(), "new entry is resident");
        assert!(cache.stats().bytes <= 128);
    }

    #[test]
    fn reinsert_replaces_accounting() {
        let cache = SharedByteLru::new(128);
        cache.insert(PageId(1), &value("first"), 64);
        cache.insert(PageId(1), &value("second"), 32);
        let stats = cache.stats();
        assert_eq!(stats.entries, 1);
        assert_eq!(stats.bytes, 32);
        assert_eq!(cache.get(PageId(1)).as_deref(), Some(&"second".to_owned()));
    }

    #[test]
    fn oversized_values_are_not_cached() {
        let cache = SharedByteLru::new(16);
        cache.insert(PageId(1), &value("way too big"), 64);
        assert!(cache.get(PageId(1)).is_none());
        assert_eq!(cache.stats().entries, 0);
    }
}
