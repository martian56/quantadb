use crate::{DurableStore, Lsn, Page, PageId, Result, StorageError};
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BufferPoolStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

struct Frame {
    page: Page,
    last_access: u64,
}

/// A bounded write-through page cache with deterministic LRU eviction.
///
/// Returning pages through a borrow of `&mut self` implicitly pins the page:
/// Rust prevents another pool operation (and therefore eviction) while the
/// borrow is alive.
pub struct BufferPool {
    store: DurableStore,
    capacity: usize,
    clock: u64,
    frames: HashMap<PageId, Frame>,
    stats: BufferPoolStats,
}

impl BufferPool {
    pub fn new(store: DurableStore, capacity: usize) -> Result<Self> {
        if capacity == 0 {
            return Err(StorageError::Configuration(
                "buffer pool capacity must be greater than zero".to_owned(),
            ));
        }
        Ok(Self {
            store,
            capacity,
            clock: 0,
            frames: HashMap::with_capacity(capacity),
            stats: BufferPoolStats::default(),
        })
    }

    pub fn get(&mut self, page_id: PageId) -> Result<Option<&Page>> {
        self.clock = self.clock.saturating_add(1);
        if self.frames.contains_key(&page_id) {
            self.stats.hits = self.stats.hits.saturating_add(1);
            let frame = self
                .frames
                .get_mut(&page_id)
                .ok_or(StorageError::PageNotFound(page_id))?;
            frame.last_access = self.clock;
            return Ok(Some(&frame.page));
        }

        self.stats.misses = self.stats.misses.saturating_add(1);
        let Some(page) = self.store.read_page(page_id)? else {
            return Ok(None);
        };
        self.make_room(page_id)?;
        self.frames.insert(
            page_id,
            Frame {
                page,
                last_access: self.clock,
            },
        );
        Ok(self.frames.get(&page_id).map(|frame| &frame.page))
    }

    pub fn allocate(&mut self, payload: impl Into<Vec<u8>>) -> Result<PageId> {
        let payload = payload.into();
        let page_id = self.store.allocate_page(payload.clone())?;
        let page = self
            .store
            .read_page(page_id)?
            .ok_or(StorageError::PageNotFound(page_id))?;
        self.insert_frame(page)?;
        Ok(page_id)
    }

    pub fn write(&mut self, page_id: PageId, payload: impl Into<Vec<u8>>) -> Result<Lsn> {
        let payload = payload.into();
        let lsn = self.store.write_page(page_id, payload.clone())?;
        let page = Page::with_lsn(page_id, lsn, payload)?;
        self.insert_frame(page)?;
        Ok(lsn)
    }

    pub fn checkpoint(&mut self) -> Result<Lsn> {
        self.store.checkpoint()
    }

    #[must_use]
    pub const fn stats(&self) -> BufferPoolStats {
        self.stats
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.frames.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    fn insert_frame(&mut self, page: Page) -> Result<()> {
        self.clock = self.clock.saturating_add(1);
        let page_id = page.id();
        if !self.frames.contains_key(&page_id) {
            self.make_room(page_id)?;
        }
        self.frames.insert(
            page_id,
            Frame {
                page,
                last_access: self.clock,
            },
        );
        Ok(())
    }

    fn make_room(&mut self, incoming: PageId) -> Result<()> {
        if self.frames.contains_key(&incoming) || self.frames.len() < self.capacity {
            return Ok(());
        }
        let victim = self
            .frames
            .iter()
            .min_by_key(|(_, frame)| frame.last_access)
            .map(|(page_id, _)| *page_id)
            .ok_or(StorageError::BufferPoolExhausted {
                capacity: self.capacity,
            })?;
        self.frames.remove(&victim);
        self.stats.evictions = self.stats.evictions.saturating_add(1);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;
    use tempfile::tempdir;

    #[test]
    fn lru_cache_is_bounded_and_tracks_hits_and_misses() {
        let directory = tempdir().expect("tempdir");
        let store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open store");
        let mut pool = BufferPool::new(store, 2).expect("pool");

        let first = pool.allocate(b"first".to_vec()).expect("allocate first");
        let second = pool.allocate(b"second".to_vec()).expect("allocate second");
        assert_eq!(
            pool.get(first)
                .expect("get first")
                .expect("first page")
                .payload(),
            b"first"
        );

        let third = pool.allocate(b"third".to_vec()).expect("allocate third");
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.stats().evictions, 1);

        assert_eq!(
            pool.get(second)
                .expect("reload second")
                .expect("second page")
                .payload(),
            b"second"
        );
        assert_eq!(pool.len(), 2);
        assert_eq!(pool.stats().misses, 1);
        assert_eq!(pool.stats().hits, 1);
        assert_ne!(first, third);
    }
}
