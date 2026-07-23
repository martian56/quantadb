use crate::node::Node;
use quantadb_storage::PageId;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex},
};

/// A bounded, shared cache of decoded index nodes.
///
/// Index generations are immutable and new nodes always land on fresh pages,
/// so a decoded node never goes stale. The only invalidation is LRU eviction
/// when the byte budget runs out; nodes from dead generations simply age out.
pub struct NodeCache {
    inner: Mutex<CacheInner>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NodeCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub entries: usize,
    pub bytes: usize,
}

struct CacheEntry {
    node: Arc<Node>,
    bytes: usize,
    tick: u64,
}

struct CacheInner {
    map: HashMap<PageId, CacheEntry>,
    recency: BTreeMap<u64, PageId>,
    next_tick: u64,
    bytes: usize,
    capacity: usize,
    hits: u64,
    misses: u64,
}

impl NodeCache {
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            inner: Mutex::new(CacheInner {
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
    pub fn stats(&self) -> NodeCacheStats {
        self.inner.lock().map_or_else(
            |_| NodeCacheStats::default(),
            |inner| NodeCacheStats {
                hits: inner.hits,
                misses: inner.misses,
                entries: inner.map.len(),
                bytes: inner.bytes,
            },
        )
    }

    pub(crate) fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        let mut inner = self.inner.lock().ok()?;
        let tick = inner.take_tick();
        if let Some(entry) = inner.map.get_mut(&page_id) {
            let node = Arc::clone(&entry.node);
            let previous = std::mem::replace(&mut entry.tick, tick);
            inner.recency.remove(&previous);
            inner.recency.insert(tick, page_id);
            inner.hits += 1;
            Some(node)
        } else {
            inner.misses += 1;
            None
        }
    }

    pub(crate) fn insert(&self, page_id: PageId, node: &Arc<Node>) {
        let bytes = node.approximate_bytes();
        let Ok(mut inner) = self.inner.lock() else {
            return;
        };
        if bytes > inner.capacity {
            return;
        }
        let tick = inner.take_tick();
        if let Some(previous) = inner.map.insert(
            page_id,
            CacheEntry {
                node: Arc::clone(node),
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

impl CacheInner {
    fn take_tick(&mut self) -> u64 {
        let tick = self.next_tick;
        self.next_tick += 1;
        tick
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(key: &[u8]) -> Arc<Node> {
        Arc::new(Node::Leaf {
            entries: vec![crate::IndexEntry {
                key: key.to_vec(),
                value: PageId(1),
            }],
            next: None,
        })
    }

    #[test]
    fn cache_serves_hits_and_counts_misses() {
        let cache = NodeCache::new(1 << 20);
        assert!(cache.get(PageId(7)).is_none());
        cache.insert(PageId(7), &leaf(b"key"));
        assert!(cache.get(PageId(7)).is_some());
        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.entries, 1);
        assert!(stats.bytes > 0);
    }

    #[test]
    fn cache_evicts_least_recently_used_within_budget() {
        let node = leaf(b"0123456789");
        let node_bytes = node.approximate_bytes();
        let cache = NodeCache::new(node_bytes * 2);

        cache.insert(PageId(1), &node);
        cache.insert(PageId(2), &leaf(b"0123456789"));
        assert!(cache.get(PageId(1)).is_some(), "page 1 must still be hot");
        cache.insert(PageId(3), &leaf(b"0123456789"));

        assert!(cache.get(PageId(1)).is_some(), "recently used survives");
        assert!(cache.get(PageId(2)).is_none(), "least recent is evicted");
        assert!(cache.get(PageId(3)).is_some(), "new entry is resident");
        assert!(cache.stats().bytes <= node_bytes * 2);
    }

    #[test]
    fn oversized_nodes_are_not_cached() {
        let node = leaf(b"payload");
        let cache = NodeCache::new(1);
        cache.insert(PageId(1), &node);
        assert!(cache.get(PageId(1)).is_none());
        assert_eq!(cache.stats().entries, 0);
    }
}
