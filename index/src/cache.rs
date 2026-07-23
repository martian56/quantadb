use crate::node::Node;
use quantadb_storage::{PageId, SharedByteLru};
use std::sync::Arc;

pub use quantadb_storage::ByteCacheStats as NodeCacheStats;

/// A bounded, shared cache of decoded index nodes.
///
/// Index generations are immutable and new nodes always land on fresh pages,
/// so a decoded node never goes stale. The only invalidation is LRU eviction
/// when the byte budget runs out; nodes from dead generations simply age out.
pub struct NodeCache {
    lru: SharedByteLru<Node>,
}

impl NodeCache {
    #[must_use]
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            lru: SharedByteLru::new(capacity_bytes),
        }
    }

    #[must_use]
    pub fn stats(&self) -> NodeCacheStats {
        self.lru.stats()
    }

    pub(crate) fn get(&self, page_id: PageId) -> Option<Arc<Node>> {
        self.lru.get(page_id)
    }

    pub(crate) fn insert(&self, page_id: PageId, node: &Arc<Node>) {
        self.lru.insert(page_id, node, node.approximate_bytes());
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
    fn cache_serves_hits_and_bounds_bytes() {
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
        assert!(cache.stats().hits >= 3);
    }
}
