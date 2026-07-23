use crate::{index_manifest::IndexGeneration, record::VersionRecord, Result, TransactionError};
use quantadb_index::{BPlusTree, IndexEntry, IndexMutation, IndexRoot, NodeCache, NodeCacheStats};
use quantadb_storage::{
    ByteCacheStats, DurableStore, GroupCommitHandle, GroupCommitOptions, GroupCommitStats,
    GroupCommitter, PageId, PageWrite, SharedByteLru, StoreOptions,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    mem,
    path::Path,
    sync::{Arc, Condvar, Mutex, MutexGuard},
    thread,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TransactionId(pub u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MvccOptions {
    pub store: StoreOptions,
    pub group_commit: GroupCommitOptions,
    /// Publish index generations from a background thread after commits.
    ///
    /// When disabled, generations advance only through explicit
    /// `rebuild_index` or `checkpoint` calls.
    pub online_index: bool,
    /// Byte budget for the shared cache of decoded index nodes.
    pub index_cache_bytes: usize,
    /// Byte budget for the shared cache of decoded version records.
    pub record_cache_bytes: usize,
}

impl Default for MvccOptions {
    fn default() -> Self {
        Self {
            store: StoreOptions::default(),
            group_commit: GroupCommitOptions::default(),
            online_index: true,
            index_cache_bytes: 64 << 20,
            record_cache_bytes: 64 << 20,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitResult {
    pub timestamp: Option<Timestamp>,
    pub writes: usize,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MvccStats {
    pub visible_through: Timestamp,
    pub keys: usize,
    pub versions: usize,
    pub active_transactions: usize,
    pub write_intents: usize,
    pub indexed_through: Option<Timestamp>,
    pub index_height: Option<u16>,
    pub indexed_keys: u64,
    pub reclaimed_versions: u64,
    /// Pages currently waiting in the store's free pool for reuse.
    pub free_pages: usize,
    pub index_cache: NodeCacheStats,
    pub record_cache: ByteCacheStats,
    pub group_commit: GroupCommitStats,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexBuildResult {
    pub timestamp: Timestamp,
    pub root: Option<IndexRoot>,
}

#[derive(Debug, Clone)]
struct Version {
    timestamp: Timestamp,
    value: Option<Vec<u8>>,
    page_id: PageId,
}

struct State {
    versions: BTreeMap<Vec<u8>, Vec<Version>>,
    intents: HashMap<Vec<u8>, TransactionId>,
    active: HashMap<TransactionId, Timestamp>,
    completed_timestamps: BTreeSet<u64>,
    next_transaction_id: u64,
    next_commit_timestamp: u64,
    visible_through: Timestamp,
    /// Published index generations, ascending by timestamp then manifest.
    ///
    /// The last entry is the newest. Older entries stay until no active
    /// snapshot could still need them, because a snapshot older than the
    /// newest generation must read reclaimed keys from a generation at or
    /// below itself.
    generations: Vec<IndexGeneration>,
    /// Keys with committed versions the newest generation may not cover.
    dirty_keys: BTreeSet<Vec<u8>>,
    /// Cumulative count of versions dropped from memory by reclamation.
    reclaimed_versions: u64,
}

impl State {
    fn latest_generation(&self) -> Option<IndexGeneration> {
        self.generations.last().copied()
    }

    /// The newest generation whose snapshot is not newer than `snapshot`.
    fn generation_at(&self, snapshot: Timestamp) -> Option<IndexGeneration> {
        self.generations
            .iter()
            .rev()
            .find(|generation| generation.timestamp <= snapshot)
            .copied()
    }

    fn oldest_active_snapshot(&self) -> Timestamp {
        self.active
            .values()
            .min()
            .copied()
            .unwrap_or(self.visible_through)
    }

    /// Record a durably committed generation, keeping the ring ordered.
    fn publish_generation(&mut self, generation: IndexGeneration) {
        let key = (generation.timestamp, generation.manifest_page_id);
        let position = self
            .generations
            .partition_point(|existing| (existing.timestamp, existing.manifest_page_id) <= key);
        let duplicate = position > 0 && {
            let previous = self.generations[position - 1];
            (previous.timestamp, previous.manifest_page_id) == key
        };
        if !duplicate {
            self.generations.insert(position, generation);
        }
    }

    /// Drop versions and generations no active or future snapshot can need.
    ///
    /// Versions above the oldest active snapshot always survive; below it,
    /// only the newest version of each key does. A key leaves the map
    /// entirely only when the covering generation, the newest one at or
    /// below the oldest active snapshot, holds its surviving version. Every
    /// current or future snapshot can then reach that value through a
    /// generation at or below itself, even after newer versions and newer
    /// generations exist. Keys with pending intents or unpublished commits
    /// are never removed. Generations older than the covering one are
    /// unreachable and leave the ring.
    /// Returns the pages nothing can reference anymore, ready for reuse.
    ///
    /// A drained version's page is unreachable: the map no longer holds it,
    /// and generation entries that still name it are masked by the map
    /// holding a version at or below every reachable snapshot for that key.
    /// A pruned generation's manifest page is unreachable because restart
    /// selection only ever uses the newest manifest. Shared copy-on-write
    /// index nodes are not freed; that needs reachability tracking.
    fn reclaim_covered(&mut self) -> Vec<PageId> {
        let mut freed = Vec::new();
        let oldest_active = self.oldest_active_snapshot();
        if let Some(cover_position) = self
            .generations
            .iter()
            .rposition(|generation| generation.timestamp <= oldest_active)
        {
            freed.extend(
                self.generations
                    .drain(..cover_position)
                    .map(|generation| generation.manifest_page_id),
            );
        }
        let cover = self
            .generation_at(oldest_active)
            .map(|generation| generation.timestamp);
        let Self {
            versions,
            intents,
            dirty_keys,
            reclaimed_versions,
            ..
        } = self;
        versions.retain(|key, key_versions| {
            if let Some(newest_visible) = key_versions
                .iter()
                .rposition(|version| version.timestamp <= oldest_active)
            {
                freed.extend(
                    key_versions
                        .drain(..newest_visible)
                        .map(|version| version.page_id),
                );
                *reclaimed_versions += newest_visible as u64;
            }
            let Some(cover) = cover else {
                return true;
            };
            let fully_covered = key_versions.len() == 1 && key_versions[0].timestamp <= cover;
            if fully_covered && !dirty_keys.contains(key) && !intents.contains_key(key) {
                // The surviving version's page stays live: the covering
                // generation points at it and serves it after this removal.
                *reclaimed_versions += 1;
                return false;
            }
            true
        });
        freed
    }
}

#[derive(Default)]
struct PublishSignal {
    dirty: bool,
    stop: bool,
}

struct Inner {
    state: Mutex<State>,
    storage: GroupCommitHandle,
    node_cache: NodeCache,
    record_cache: SharedByteLru<VersionRecord>,
    publish_signal: Mutex<PublishSignal>,
    publish_wake: Condvar,
}

pub struct MvccDatabase {
    inner: Arc<Inner>,
    committer: Option<GroupCommitter>,
    publisher: Option<thread::JoinHandle<()>>,
}

pub struct Transaction {
    inner: Arc<Inner>,
    id: TransactionId,
    snapshot: Timestamp,
    writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    /// Prefixes this transaction scanned while range protection was on.
    ///
    /// A cell so read paths stay shared borrows; a transaction is owned by
    /// one thread at a time, never shared.
    scanned_prefixes: std::cell::RefCell<Vec<Vec<u8>>>,
    range_protected: bool,
    finished: bool,
}

impl MvccDatabase {
    pub fn open(path: impl AsRef<Path>, options: MvccOptions) -> Result<Self> {
        let mut store = DurableStore::open(path, options.store)?;
        let mut versions: BTreeMap<Vec<u8>, Vec<Version>> = BTreeMap::new();
        let mut index_generations = Vec::new();
        let mut maximum_timestamp = 0_u64;

        for raw_page_id in 0..store.page_count()? {
            let page_id = PageId(raw_page_id);
            let Some(page) = store.read_page(page_id)? else {
                continue;
            };
            if let Some(generation) = IndexGeneration::decode(page_id, page.payload())? {
                index_generations.push(generation);
                continue;
            }
            let Some(record) = VersionRecord::decode(page_id, page.payload())? else {
                continue;
            };
            maximum_timestamp = maximum_timestamp.max(record.timestamp.0);
            let key_versions = versions.entry(record.key).or_default();
            if key_versions
                .iter()
                .any(|version| version.timestamp == record.timestamp)
            {
                return Err(TransactionError::CorruptRecord {
                    page_id,
                    reason: format!(
                        "duplicate version timestamp {} for one key",
                        record.timestamp.0
                    ),
                });
            }
            key_versions.push(Version {
                timestamp: record.timestamp,
                value: record.value,
                page_id,
            });
        }
        for key_versions in versions.values_mut() {
            key_versions.sort_unstable_by_key(|version| version.timestamp);
        }

        let next_commit_timestamp = maximum_timestamp
            .checked_add(1)
            .ok_or(TransactionError::TimestampExhausted)?;
        let mut generations = index_generations
            .into_iter()
            .filter(|generation| generation.timestamp.0 <= maximum_timestamp)
            .collect::<Vec<_>>();
        generations
            .sort_unstable_by_key(|generation| (generation.timestamp, generation.manifest_page_id));
        let committer = GroupCommitter::start(store, options.group_commit)?;
        let storage = committer.handle();
        if let Some(root) = generations.last().and_then(|generation| generation.root) {
            BPlusTree::range(&storage, None, root, None, None, 1)?;
        }
        let indexed_through = generations
            .last()
            .map_or(Timestamp(0), |generation| generation.timestamp);
        let dirty_keys: BTreeSet<Vec<u8>> = versions
            .iter()
            .filter(|(_, versions)| {
                versions
                    .iter()
                    .any(|version| version.timestamp > indexed_through)
            })
            .map(|(key, _)| key.clone())
            .collect();
        let needs_catch_up = !dirty_keys.is_empty();
        let mut state = State {
            versions,
            intents: HashMap::new(),
            active: HashMap::new(),
            completed_timestamps: BTreeSet::new(),
            next_transaction_id: 1,
            next_commit_timestamp,
            visible_through: Timestamp(maximum_timestamp),
            generations,
            dirty_keys,
            reclaimed_versions: 0,
        };
        let freed_at_open = state.reclaim_covered();
        storage.release_pages(freed_at_open)?;
        let inner = Arc::new(Inner {
            state: Mutex::new(state),
            storage,
            node_cache: NodeCache::new(options.index_cache_bytes),
            record_cache: SharedByteLru::new(options.record_cache_bytes),
            publish_signal: Mutex::new(PublishSignal::default()),
            publish_wake: Condvar::new(),
        });
        let publisher = options.online_index.then(|| {
            let worker = Arc::clone(&inner);
            thread::spawn(move || worker.publish_loop())
        });
        let database = Self {
            inner,
            committer: Some(committer),
            publisher,
        };
        if options.online_index && needs_catch_up {
            database.inner.mark_index_dirty();
        }
        Ok(database)
    }

    pub fn begin(&self) -> Result<Transaction> {
        let mut state = self.inner.lock_state()?;
        let id = TransactionId(state.next_transaction_id);
        state.next_transaction_id = state
            .next_transaction_id
            .checked_add(1)
            .ok_or(TransactionError::TransactionIdExhausted)?;
        let snapshot = state.visible_through;
        state.active.insert(id, snapshot);
        drop(state);

        Ok(Transaction {
            inner: Arc::clone(&self.inner),
            id,
            snapshot,
            writes: BTreeMap::new(),
            scanned_prefixes: std::cell::RefCell::new(Vec::new()),
            range_protected: false,
            finished: false,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let snapshot = self.inner.lock_state()?.visible_through;
        self.inner.read(key, snapshot)
    }

    pub fn checkpoint(&self) -> Result<Timestamp> {
        let generation = self.rebuild_index()?;
        self.inner.storage.checkpoint()?;
        Ok(generation.timestamp)
    }

    pub fn rebuild_index(&self) -> Result<IndexBuildResult> {
        self.inner.rebuild_index()
    }

    pub fn stats(&self) -> Result<MvccStats> {
        let state = self.inner.lock_state()?;
        Ok(MvccStats {
            visible_through: state.visible_through,
            keys: state.versions.len(),
            versions: state.versions.values().map(Vec::len).sum(),
            active_transactions: state.active.len(),
            write_intents: state.intents.len(),
            indexed_through: state
                .latest_generation()
                .map(|generation| generation.timestamp),
            index_height: state
                .latest_generation()
                .and_then(|generation| generation.root)
                .map(|root| root.height),
            indexed_keys: state
                .latest_generation()
                .and_then(|generation| generation.root)
                .map_or(0, |root| root.entries),
            reclaimed_versions: state.reclaimed_versions,
            free_pages: self.inner.storage.free_page_count()?,
            index_cache: self.inner.node_cache.stats(),
            record_cache: self.inner.record_cache.stats(),
            group_commit: self.inner.storage.stats(),
        })
    }

    pub fn shutdown(mut self) -> Result<()> {
        self.stop_publisher();
        if let Some(committer) = self.committer.take() {
            committer.shutdown()?;
        }
        Ok(())
    }

    fn stop_publisher(&mut self) {
        let Some(handle) = self.publisher.take() else {
            return;
        };
        if let Ok(mut signal) = self.inner.publish_signal.lock() {
            signal.stop = true;
        }
        self.inner.publish_wake.notify_all();
        let _ = handle.join();
    }
}

impl Drop for MvccDatabase {
    fn drop(&mut self) {
        self.stop_publisher();
    }
}

impl Inner {
    fn lock_state(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| TransactionError::StatePoisoned)
    }

    /// Background thread body: rebuild the persistent generation whenever
    /// commits mark the index dirty, until shutdown.
    ///
    /// A failed rebuild is retried on the next commit signal instead of
    /// looping; the in-memory version map keeps every read correct while the
    /// persistent generation lags.
    fn publish_loop(&self) {
        loop {
            let Ok(mut signal) = self.publish_signal.lock() else {
                return;
            };
            while !signal.dirty && !signal.stop {
                let Ok(next) = self.publish_wake.wait(signal) else {
                    return;
                };
                signal = next;
            }
            if signal.stop {
                return;
            }
            signal.dirty = false;
            drop(signal);
            let _ = self.rebuild_index();
            // A short pause coalesces bursts: under sustained commits the
            // publisher folds many deltas per build instead of contending
            // for the pipeline after every single one.
            thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    fn mark_index_dirty(&self) {
        if let Ok(mut signal) = self.publish_signal.lock() {
            signal.dirty = true;
        }
        self.publish_wake.notify_one();
    }

    fn rebuild_index(&self) -> Result<IndexBuildResult> {
        let (timestamp, base_generation, live_entries, mutations) = {
            let state = self.lock_state()?;
            let timestamp = state.visible_through;
            let base_generation = state.latest_generation();
            if base_generation.is_some_and(|generation| generation.timestamp == timestamp) {
                return Ok(IndexBuildResult {
                    timestamp,
                    root: base_generation.and_then(|generation| generation.root),
                });
            }
            let live_entries = base_generation
                .is_none()
                .then(|| current_index_entries(&state.versions, timestamp).collect::<Vec<_>>());
            let mutations = base_generation.map(|generation| {
                state
                    .dirty_keys
                    .iter()
                    .filter_map(|key| {
                        let newest = state
                            .versions
                            .get(key)?
                            .iter()
                            .rev()
                            .find(|version| version.timestamp <= timestamp)?;
                        if newest.timestamp <= generation.timestamp {
                            return None;
                        }
                        Some(newest.value.as_ref().map_or_else(
                            || IndexMutation::Delete(key.clone()),
                            |_| {
                                IndexMutation::Upsert(IndexEntry {
                                    key: key.clone(),
                                    value: newest.page_id,
                                })
                            },
                        ))
                    })
                    .collect::<Vec<_>>()
            });
            (timestamp, base_generation, live_entries, mutations)
        };
        let plan = if let Some(generation) = base_generation {
            BPlusTree::edit_plan(
                &self.storage,
                Some(&self.node_cache),
                generation.root,
                mutations.unwrap_or_default(),
            )?
        } else {
            BPlusTree::plan(&self.storage, live_entries.unwrap_or_default())?
        };
        let root = plan.root();
        let manifest_page_id = self
            .storage
            .reserve_page_ids(1)?
            .into_iter()
            .next()
            .ok_or_else(|| {
                TransactionError::Storage(quantadb_storage::StorageError::GroupCommit(
                    "manifest page reservation returned no page".to_owned(),
                ))
            })?;
        let generation = IndexGeneration {
            timestamp,
            root,
            manifest_page_id,
        };
        let mut writes = plan.into_writes();
        writes.push(PageWrite {
            page_id: manifest_page_id,
            payload: generation.encode(),
        });
        // The generation rides the next foreground sync instead of forcing
        // its own: readers reach the new nodes through the dirty table
        // right away, and if a crash loses the batch, restart selects the
        // previous durable manifest and the dirty-key seed rebuilds this
        // one. Publication therefore adds no fsync to the commit path.
        self.storage.commit_relaxed(writes)?;

        let mut state = self.lock_state()?;
        state.publish_generation(generation);
        let State {
            versions,
            dirty_keys,
            ..
        } = &mut *state;
        dirty_keys.retain(|key| {
            versions.get(key).is_some_and(|versions| {
                versions.iter().any(|version| version.timestamp > timestamp)
            })
        });
        let freed = state.reclaim_covered();
        drop(state);
        // Freed pages can be reused with new content, so any cached decode
        // of their old content has to go before the IDs recirculate.
        for page_id in &freed {
            self.record_cache.remove(*page_id);
        }
        self.storage.release_pages(freed)?;
        Ok(IndexBuildResult { timestamp, root })
    }

    /// Read one key at a snapshot.
    ///
    /// The version map answers whenever it holds a version at or below the
    /// snapshot. Otherwise the newest generation at or below the snapshot
    /// answers: reclamation only removes versions the covering generation
    /// holds, and the ring keeps every generation an active snapshot could
    /// still need, so the value this snapshot saw at begin stays reachable
    /// even after newer versions and newer generations appear. A key that
    /// truly did not exist at the snapshot is in neither place, and
    /// `read_index_version` rejects any record newer than the snapshot as
    /// corruption.
    fn read(&self, key: &[u8], snapshot: Timestamp) -> Result<Option<Vec<u8>>> {
        let state = self.lock_state()?;
        if let Some(version) = state.versions.get(key).and_then(|versions| {
            versions
                .iter()
                .rev()
                .find(|version| version.timestamp <= snapshot)
        }) {
            return Ok(version.value.clone());
        }
        let root = state
            .generation_at(snapshot)
            .map(|generation| generation.root);
        drop(state);
        root.map_or(Ok(None), |root| self.read_indexed(root, key, snapshot))
    }

    fn read_indexed(
        &self,
        root: Option<IndexRoot>,
        key: &[u8],
        snapshot: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        let Some(root) = root else {
            return Ok(None);
        };
        let Some(page_id) = BPlusTree::get(&self.storage, Some(&self.node_cache), root, key)?
        else {
            return Ok(None);
        };
        self.read_index_version(page_id, key, snapshot)
    }

    fn read_index_version(
        &self,
        page_id: PageId,
        key: &[u8],
        snapshot: Timestamp,
    ) -> Result<Option<Vec<u8>>> {
        let record = match self.record_cache.get(page_id) {
            Some(record) => record,
            None => {
                let page = self.storage.read_page(page_id)?.ok_or_else(|| {
                    TransactionError::CorruptRecord {
                        page_id,
                        reason: "index points to an absent version page".to_owned(),
                    }
                })?;
                let record = VersionRecord::decode(page_id, page.payload())?.ok_or_else(|| {
                    TransactionError::CorruptRecord {
                        page_id,
                        reason: "index points to a non-MVCC page".to_owned(),
                    }
                })?;
                let record = Arc::new(record);
                self.record_cache
                    .insert(page_id, &record, record.approximate_bytes());
                record
            }
        };
        if record.key != key {
            return Err(TransactionError::CorruptRecord {
                page_id,
                reason: "index key does not match version-record key".to_owned(),
            });
        }
        if record.timestamp > snapshot || record.value.is_none() {
            return Err(TransactionError::CorruptRecord {
                page_id,
                reason: "index points to a version that is not live in its snapshot".to_owned(),
            });
        }
        Ok(record.value.clone())
    }

    fn prepare_commit(
        &self,
        transaction_id: TransactionId,
        snapshot: Timestamp,
        keys: impl Iterator<Item = Vec<u8>>,
        scanned_prefixes: &[Vec<u8>],
    ) -> Result<Timestamp> {
        let keys = keys.collect::<Vec<_>>();
        let mut state = self.lock_state()?;

        for key in &keys {
            if state
                .intents
                .get(key)
                .is_some_and(|owner| *owner != transaction_id)
            {
                return Err(TransactionError::WriteConflict { key: key.clone() });
            }
            if state
                .versions
                .get(key)
                .and_then(|versions| versions.last())
                .is_some_and(|version| version.timestamp > snapshot)
            {
                return Err(TransactionError::WriteConflict { key: key.clone() });
            }
        }

        // First-committer-wins for predicates: a protected scan conflicts
        // with any commit, or in-flight intent, that landed under one of
        // its prefixes after the snapshot. Reclamation never removes
        // versions above the oldest active snapshot, so every version this
        // check needs is still in the map.
        for prefix in scanned_prefixes {
            let newer_version = version_range(&state.versions, prefix).any(|(_, versions)| {
                versions
                    .last()
                    .is_some_and(|version| version.timestamp > snapshot)
            });
            let foreign_intent = state
                .intents
                .iter()
                .any(|(key, owner)| *owner != transaction_id && key.starts_with(prefix.as_slice()));
            if newer_version || foreign_intent {
                return Err(TransactionError::RangeConflict {
                    prefix: prefix.clone(),
                });
            }
        }

        let timestamp = Timestamp(state.next_commit_timestamp);
        state.next_commit_timestamp = state
            .next_commit_timestamp
            .checked_add(1)
            .ok_or(TransactionError::TimestampExhausted)?;
        for key in keys {
            state.intents.insert(key, transaction_id);
        }
        Ok(timestamp)
    }

    fn persist_commit(
        &self,
        timestamp: Timestamp,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
    ) -> Result<Vec<PageId>> {
        let page_ids = self.storage.reserve_page_ids(writes.len())?;
        let page_writes = writes
            .iter()
            .zip(&page_ids)
            .map(|((key, value), page_id)| {
                let record = VersionRecord {
                    timestamp,
                    key: key.clone(),
                    value: value.clone(),
                };
                let payload = record.encode()?;
                if record.value.is_some() {
                    let record = Arc::new(record);
                    self.record_cache
                        .insert(*page_id, &record, record.approximate_bytes());
                }
                Ok(PageWrite {
                    page_id: *page_id,
                    payload,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let lsns = self.storage.commit(page_writes)?;
        if lsns.len() != writes.len() {
            return Err(TransactionError::Storage(
                quantadb_storage::StorageError::GroupCommit(format!(
                    "commit returned {} LSNs for {} writes",
                    lsns.len(),
                    writes.len()
                )),
            ));
        }
        Ok(page_ids)
    }

    fn finish_prepared(
        &self,
        transaction_id: TransactionId,
        timestamp: Timestamp,
        writes: &[(Vec<u8>, Option<Vec<u8>>)],
        page_ids: Option<&[PageId]>,
    ) -> Result<()> {
        let mut state = self.lock_state()?;
        for (key, _) in writes {
            if state.intents.get(key) == Some(&transaction_id) {
                state.intents.remove(key);
            }
        }

        if let Some(page_ids) = page_ids {
            for (((key, value), page_id), position) in writes.iter().zip(page_ids).zip(0_usize..) {
                let versions = state.versions.entry(key.clone()).or_default();
                let insert_at = versions
                    .binary_search_by_key(&timestamp, |version| version.timestamp)
                    .unwrap_or_else(|position| position);
                if versions
                    .get(insert_at)
                    .is_some_and(|version| version.timestamp == timestamp)
                {
                    return Err(TransactionError::CorruptRecord {
                        page_id: *page_id,
                        reason: format!(
                            "duplicate commit timestamp {} at write {position}",
                            timestamp.0
                        ),
                    });
                }
                versions.insert(
                    insert_at,
                    Version {
                        timestamp,
                        value: value.clone(),
                        page_id: *page_id,
                    },
                );
                state.dirty_keys.insert(key.clone());
            }
        }

        state.completed_timestamps.insert(timestamp.0);
        while let Some(next) = state.visible_through.0.checked_add(1) {
            if !state.completed_timestamps.remove(&next) {
                break;
            }
            state.visible_through = Timestamp(next);
        }
        Ok(())
    }

    fn end_transaction(&self, transaction_id: TransactionId) -> Result<()> {
        self.lock_state()?.active.remove(&transaction_id);
        Ok(())
    }

    fn commit(
        &self,
        transaction_id: TransactionId,
        snapshot: Timestamp,
        writes: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
        scanned_prefixes: &[Vec<u8>],
    ) -> Result<CommitResult> {
        if writes.is_empty() {
            return Ok(CommitResult {
                timestamp: None,
                writes: 0,
            });
        }
        let writes = writes.into_iter().collect::<Vec<_>>();
        let timestamp = self.prepare_commit(
            transaction_id,
            snapshot,
            writes.iter().map(|(key, _)| key.clone()),
            scanned_prefixes,
        )?;

        match self.persist_commit(timestamp, &writes) {
            Ok(page_ids) => {
                self.finish_prepared(transaction_id, timestamp, &writes, Some(&page_ids))?;
                self.mark_index_dirty();
                Ok(CommitResult {
                    timestamp: Some(timestamp),
                    writes: writes.len(),
                })
            }
            Err(error) => {
                let _ = self.finish_prepared(transaction_id, timestamp, &writes, None);
                Err(error)
            }
        }
    }
}

impl Transaction {
    #[must_use]
    pub const fn id(&self) -> TransactionId {
        self.id
    }

    #[must_use]
    pub const fn snapshot(&self) -> Timestamp {
        self.snapshot
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.ensure_active()?;
        if let Some(value) = self.writes.get(key) {
            return Ok(value.clone());
        }
        self.inner.read(key, self.snapshot)
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Result<()> {
        self.ensure_active()?;
        let key = key.into();
        let value = value.into();
        VersionRecord::validate_size(&key, Some(&value))?;
        self.writes.insert(key, Some(value));
        Ok(())
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> Result<()> {
        self.ensure_active()?;
        let key = key.into();
        VersionRecord::validate_size(&key, None)?;
        self.writes.insert(key, None);
        Ok(())
    }

    /// Track scanned prefixes and refuse to commit writes if another
    /// transaction commits anything under them first.
    ///
    /// Snapshot isolation alone does not prevent phantoms: two transactions
    /// can scan the same range, see the same rows, and insert disjoint keys
    /// that each invalidate the other's reasoning. With protection on, the
    /// first committer wins and the second aborts with a range conflict,
    /// the same policy point writes already follow. Read-only commits are
    /// unaffected; a snapshot read is already consistent.
    pub fn protect_scans(&mut self) {
        self.range_protected = true;
    }

    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.ensure_active()?;
        if self.range_protected {
            let mut scanned = self.scanned_prefixes.borrow_mut();
            if !scanned
                .iter()
                .any(|existing| prefix.starts_with(existing.as_slice()))
            {
                scanned.retain(|existing| !existing.starts_with(prefix));
                scanned.push(prefix.to_vec());
            }
        }
        let state = self.inner.lock_state()?;
        let mut masked = BTreeSet::new();
        let mut results = BTreeMap::new();
        for (key, versions) in version_range(&state.versions, prefix) {
            let Some(version) = versions
                .iter()
                .rev()
                .find(|version| version.timestamp <= self.snapshot)
            else {
                continue;
            };
            masked.insert(key.clone());
            if let Some(value) = version.value.clone() {
                results.insert(key.clone(), value);
            }
        }
        let root = state
            .generation_at(self.snapshot)
            .and_then(|generation| generation.root);
        drop(state);

        if let Some(root) = root {
            let end = prefix_end(prefix);
            let entries = BPlusTree::range(
                &self.inner.storage,
                Some(&self.inner.node_cache),
                root,
                Some(prefix),
                end.as_deref(),
                usize::MAX,
            )?;
            for entry in entries {
                if masked.contains(&entry.key) {
                    continue;
                }
                if let Some(value) =
                    self.inner
                        .read_index_version(entry.value, &entry.key, self.snapshot)?
                {
                    results.insert(entry.key, value);
                }
            }
        }

        for (key, value) in self
            .writes
            .iter()
            .filter(|(key, _)| key.starts_with(prefix))
        {
            if let Some(value) = value {
                results.insert(key.clone(), value.clone());
            } else {
                results.remove(key);
            }
        }
        Ok(results.into_iter().collect())
    }

    pub fn commit(mut self) -> Result<CommitResult> {
        self.ensure_active()?;
        let writes = mem::take(&mut self.writes);
        let prefixes = self.scanned_prefixes.take();
        let result = self.inner.commit(self.id, self.snapshot, writes, &prefixes);
        self.finished = true;
        self.inner.end_transaction(self.id)?;
        result
    }

    pub fn rollback(mut self) -> Result<()> {
        self.ensure_active()?;
        self.writes.clear();
        self.finished = true;
        self.inner.end_transaction(self.id)
    }

    fn ensure_active(&self) -> Result<()> {
        if self.finished {
            Err(TransactionError::Inactive)
        } else {
            Ok(())
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.inner.end_transaction(self.id);
            self.finished = true;
        }
    }
}

fn current_index_entries(
    versions: &BTreeMap<Vec<u8>, Vec<Version>>,
    snapshot: Timestamp,
) -> impl Iterator<Item = IndexEntry> + '_ {
    versions.iter().filter_map(move |(key, versions)| {
        versions
            .iter()
            .rev()
            .find(|version| version.timestamp <= snapshot)
            .and_then(|version| {
                version.value.as_ref().map(|_| IndexEntry {
                    key: key.clone(),
                    value: version.page_id,
                })
            })
    })
}

fn version_range<'a>(
    versions: &'a BTreeMap<Vec<u8>, Vec<Version>>,
    prefix: &[u8],
) -> impl Iterator<Item = (&'a Vec<u8>, &'a Vec<Version>)> {
    let end = prefix_end(prefix);
    versions.range::<[u8], _>((std::ops::Bound::Included(prefix), end_bound(end.as_deref())))
}

fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for position in (0..end.len()).rev() {
        if end[position] != u8::MAX {
            end[position] += 1;
            end.truncate(position + 1);
            return Some(end);
        }
    }
    None
}

fn end_bound(end: Option<&[u8]>) -> std::ops::Bound<&[u8]> {
    end.map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::HashMap,
        sync::{Arc, Barrier},
        thread,
        time::Duration,
    };
    use tempfile::tempdir;

    /// Open with background publication disabled so index timing assertions
    /// stay deterministic. Online behavior has its own tests.
    fn open_database(path: &Path) -> MvccDatabase {
        MvccDatabase::open(path, offline_options()).expect("open database")
    }

    fn offline_options() -> MvccOptions {
        MvccOptions {
            online_index: false,
            ..MvccOptions::default()
        }
    }

    fn wait_for_indexed_through(database: &MvccDatabase, target: Timestamp) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let stats = database.stats().expect("stats");
            if stats.indexed_through == Some(target) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "publisher stalled at {:?} before reaching {target:?}",
                stats.indexed_through
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn committed_values_and_tombstones_survive_restart() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut transaction = database.begin().expect("begin");
            transaction.put(b"a", b"one").expect("put");
            transaction.put(b"b", b"two").expect("put");
            assert_eq!(
                transaction.commit().expect("commit").timestamp,
                Some(Timestamp(1))
            );

            let mut deletion = database.begin().expect("begin");
            deletion.delete(b"b").expect("delete");
            deletion.commit().expect("commit deletion");
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        assert_eq!(database.get(b"a").expect("get"), Some(b"one".to_vec()));
        assert_eq!(database.get(b"b").expect("get"), None);
        assert_eq!(
            database.stats().expect("stats").visible_through,
            Timestamp(2)
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn snapshots_are_repeatable_and_new_transactions_see_commits() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let old_snapshot = database.begin().expect("old snapshot");

        let mut writer = database.begin().expect("writer");
        writer.put(b"key", b"new").expect("put");
        writer.commit().expect("commit");

        assert_eq!(old_snapshot.get(b"key").expect("old read"), None);
        assert_eq!(
            database
                .begin()
                .expect("new snapshot")
                .get(b"key")
                .expect("new read"),
            Some(b"new".to_vec())
        );
    }

    #[test]
    fn first_committer_wins_write_conflicts() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut first = database.begin().expect("first");
        let mut second = database.begin().expect("second");
        first.put(b"key", b"first").expect("put");
        second.put(b"key", b"second").expect("put");

        first.commit().expect("first commit");
        assert!(matches!(
            second.commit(),
            Err(TransactionError::WriteConflict { .. })
        ));
        assert_eq!(database.get(b"key").expect("get"), Some(b"first".to_vec()));
    }

    #[test]
    fn read_your_writes_prefix_scan_and_rollback_work() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut transaction = database.begin().expect("begin");
        transaction.put(b"user:1", b"Ada").expect("put");
        transaction.put(b"user:2", b"Grace").expect("put");
        transaction.put(b"other", b"x").expect("put");
        transaction.delete(b"user:2").expect("delete");

        assert_eq!(
            transaction.get(b"user:1").expect("get"),
            Some(b"Ada".to_vec())
        );
        assert_eq!(
            transaction.scan_prefix(b"user:").expect("scan"),
            vec![(b"user:1".to_vec(), b"Ada".to_vec())]
        );
        transaction.rollback().expect("rollback");
        assert_eq!(database.get(b"user:1").expect("get"), None);
    }

    #[test]
    fn disjoint_concurrent_commits_share_storage_groups() {
        let directory = tempdir().expect("tempdir");
        let database = MvccDatabase::open(
            directory.path(),
            MvccOptions {
                store: StoreOptions::default(),
                group_commit: GroupCommitOptions {
                    queue_depth: 32,
                    max_batch_pages: 32,
                    max_delay: Duration::from_millis(25),
                    ..GroupCommitOptions::default()
                },
                ..offline_options()
            },
        )
        .expect("open");
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();

        for key in 0..8_u64 {
            let mut transaction = database.begin().expect("begin");
            transaction
                .put(key.to_le_bytes(), key.to_be_bytes())
                .expect("put");
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                transaction.commit().expect("commit")
            }));
        }
        barrier.wait();
        for thread in threads {
            assert_eq!(thread.join().expect("thread").writes, 1);
        }

        let stats = database.stats().expect("stats");
        assert_eq!(stats.visible_through, Timestamp(8));
        assert!(stats.group_commit.groups < 8, "{stats:?}");
    }

    #[test]
    fn online_publication_converges_without_checkpoints() {
        let directory = tempdir().expect("tempdir");
        let database = MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open");
        for round in 0..5_u64 {
            let mut transaction = database.begin().expect("begin");
            transaction
                .put(format!("user:{round}").into_bytes(), round.to_be_bytes())
                .expect("put");
            transaction.commit().expect("commit");
        }

        wait_for_indexed_through(&database, Timestamp(5));
        let stats = database.stats().expect("stats");
        assert_eq!(stats.indexed_keys, 5);
        assert_eq!(
            database.get(b"user:3").expect("get"),
            Some(3_u64.to_be_bytes().to_vec())
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn online_publication_covers_deletes_and_restart_catch_up() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut seed = database.begin().expect("begin seed");
            seed.put(b"keep", b"kept").expect("put keep");
            seed.put(b"drop", b"doomed").expect("put drop");
            seed.commit().expect("commit seed");
            database.rebuild_index().expect("rebuild");

            let mut deletion = database.begin().expect("begin delete");
            deletion.delete(b"drop").expect("delete");
            deletion.commit().expect("commit delete");
            assert_eq!(
                database.stats().expect("stats").indexed_through,
                Some(Timestamp(1)),
                "offline database must not publish on its own"
            );
            database.shutdown().expect("shutdown");
        }

        let database =
            MvccDatabase::open(directory.path(), MvccOptions::default()).expect("reopen");
        wait_for_indexed_through(&database, Timestamp(2));
        let stats = database.stats().expect("stats");
        assert_eq!(stats.indexed_keys, 1);
        assert_eq!(database.get(b"keep").expect("get"), Some(b"kept".to_vec()));
        assert_eq!(database.get(b"drop").expect("get"), None);
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn older_generation_reads_merge_newer_versions() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut first = database.begin().expect("begin first");
        first.put(b"row:1", b"one").expect("put");
        first.commit().expect("commit first");
        database.rebuild_index().expect("rebuild");

        let mut second = database.begin().expect("begin second");
        second.put(b"row:1", b"newer").expect("update");
        second.put(b"row:2", b"two").expect("insert");
        second.commit().expect("commit second");

        let mut third = database.begin().expect("begin third");
        third.delete(b"row:1").expect("delete");
        third.commit().expect("commit third");

        assert_eq!(
            database.stats().expect("stats").indexed_through,
            Some(Timestamp(1)),
            "generation must lag the snapshot for this test to mean anything"
        );
        assert_eq!(database.get(b"row:1").expect("get"), None);
        assert_eq!(database.get(b"row:2").expect("get"), Some(b"two".to_vec()));
        let transaction = database.begin().expect("begin scan");
        assert_eq!(
            transaction.scan_prefix(b"row:").expect("scan"),
            vec![(b"row:2".to_vec(), b"two".to_vec())]
        );
        transaction.rollback().expect("rollback");
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn concurrent_commits_converge_online() {
        let directory = tempdir().expect("tempdir");
        let database = MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open");

        thread::scope(|scope| {
            for worker in 0..4_u64 {
                let database = &database;
                scope.spawn(move || {
                    for round in 0..4_u64 {
                        let mut transaction = database.begin().expect("begin");
                        let key = format!("w{worker}:r{round}").into_bytes();
                        transaction.put(key, round.to_be_bytes()).expect("put");
                        transaction.commit().expect("commit");
                    }
                });
            }
        });

        let visible = database.stats().expect("stats").visible_through;
        assert_eq!(visible, Timestamp(16));
        wait_for_indexed_through(&database, visible);
        let stats = database.stats().expect("stats");
        assert_eq!(stats.indexed_keys, 16);
        assert_eq!(
            database.get(b"w2:r3").expect("get"),
            Some(3_u64.to_be_bytes().to_vec())
        );
        database.shutdown().expect("shutdown");
    }

    fn wait_for_map_keys(database: &MvccDatabase, target: usize) {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let stats = database.stats().expect("stats");
            if stats.keys == target {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "reclamation stalled with {} keys in the map, wanted {target}",
                stats.keys
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    #[test]
    fn cold_keys_leave_the_version_map_and_read_from_the_index() {
        let directory = tempdir().expect("tempdir");
        let database = MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open");
        for round in 0..20_u64 {
            let mut transaction = database.begin().expect("begin");
            transaction
                .put(format!("cold:{round:02}").into_bytes(), round.to_be_bytes())
                .expect("put");
            transaction.commit().expect("commit");
        }

        wait_for_indexed_through(&database, Timestamp(20));
        wait_for_map_keys(&database, 0);
        let stats = database.stats().expect("stats");
        assert_eq!(stats.versions, 0, "no version history should stay resident");
        assert!(stats.reclaimed_versions >= 20, "{stats:?}");
        assert_eq!(stats.indexed_keys, 20);

        assert_eq!(
            database.get(b"cold:07").expect("get"),
            Some(7_u64.to_be_bytes().to_vec())
        );
        let transaction = database.begin().expect("begin");
        let scanned = transaction.scan_prefix(b"cold:").expect("scan");
        assert_eq!(scanned.len(), 20);
        assert_eq!(scanned[3].0, b"cold:03".to_vec());
        transaction.rollback().expect("rollback");

        let stats = database.stats().expect("stats");
        assert!(
            stats.record_cache.hits > 0,
            "fall-through reads must be served from the record cache: {stats:?}"
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn active_snapshots_pin_the_versions_they_can_see() {
        let directory = tempdir().expect("tempdir");
        let database = MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open");
        let mut seed = database.begin().expect("begin seed");
        seed.put(b"pinned", b"first").expect("put");
        seed.commit().expect("commit seed");

        let reader = database.begin().expect("begin reader");
        assert_eq!(reader.get(b"pinned").expect("get"), Some(b"first".to_vec()));

        for value in [b"second".as_slice(), b"third".as_slice()] {
            let mut writer = database.begin().expect("begin writer");
            writer.put(b"pinned", value).expect("put");
            writer.commit().expect("commit");
        }
        wait_for_indexed_through(&database, Timestamp(3));

        let stats = database.stats().expect("stats");
        assert!(
            stats.versions >= 2,
            "the reader's snapshot must pin old history: {stats:?}"
        );
        assert_eq!(
            reader.get(b"pinned").expect("pinned read"),
            Some(b"first".to_vec()),
            "reclamation must never change what an open snapshot sees"
        );
        reader.rollback().expect("rollback reader");

        let mut unblock = database.begin().expect("begin unblock");
        unblock.put(b"other", b"value").expect("put");
        unblock.commit().expect("commit unblock");
        wait_for_indexed_through(&database, Timestamp(4));
        wait_for_map_keys(&database, 0);
        assert_eq!(
            database.get(b"pinned").expect("get"),
            Some(b"third".to_vec())
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn tombstones_are_reclaimed_and_stay_dead_after_restart() {
        let directory = tempdir().expect("tempdir");
        {
            let database =
                MvccDatabase::open(directory.path(), MvccOptions::default()).expect("open");
            let mut seed = database.begin().expect("begin seed");
            seed.put(b"keep", b"kept").expect("put keep");
            seed.put(b"gone", b"doomed").expect("put gone");
            seed.commit().expect("commit seed");
            let mut deletion = database.begin().expect("begin delete");
            deletion.delete(b"gone").expect("delete");
            deletion.commit().expect("commit delete");

            wait_for_indexed_through(&database, Timestamp(2));
            wait_for_map_keys(&database, 0);
            assert_eq!(database.get(b"gone").expect("get"), None);
            database.shutdown().expect("shutdown");
        }

        let database =
            MvccDatabase::open(directory.path(), MvccOptions::default()).expect("reopen");
        let stats = database.stats().expect("stats");
        assert_eq!(
            stats.keys, 0,
            "the open sweep must trim everything the page scan resurrected"
        );
        assert_eq!(database.get(b"gone").expect("get"), None);
        assert_eq!(database.get(b"keep").expect("get"), Some(b"kept".to_vec()));
        let transaction = database.begin().expect("begin");
        assert_eq!(
            transaction.scan_prefix(b"").expect("scan"),
            vec![(b"keep".to_vec(), b"kept".to_vec())]
        );
        transaction.rollback().expect("rollback");
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn old_snapshots_read_reclaimed_keys_from_an_older_generation() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut seed = database.begin().expect("begin seed");
        seed.put(b"racy", b"first").expect("put racy");
        seed.commit().expect("commit seed");
        database.rebuild_index().expect("first rebuild");

        let mut filler = database.begin().expect("begin filler");
        filler.put(b"other", b"noise").expect("put other");
        filler.commit().expect("commit filler");
        database.rebuild_index().expect("second rebuild");
        assert_eq!(
            database.stats().expect("stats").keys,
            0,
            "with no active snapshots both keys must be fully reclaimed"
        );

        let reader = database.begin().expect("begin reader");
        assert_eq!(reader.snapshot(), Timestamp(2));

        let mut rewrite = database.begin().expect("begin rewrite");
        rewrite.put(b"racy", b"second").expect("rewrite racy");
        rewrite.commit().expect("commit rewrite");
        database.rebuild_index().expect("third rebuild");

        assert_eq!(
            reader.get(b"racy").expect("pinned read"),
            Some(b"first".to_vec()),
            "the old snapshot must read the reclaimed version from an older generation"
        );
        let scanned = reader.scan_prefix(b"").expect("pinned scan");
        assert_eq!(
            scanned,
            vec![
                (b"other".to_vec(), b"noise".to_vec()),
                (b"racy".to_vec(), b"first".to_vec()),
            ],
            "the old snapshot's scan must merge map and older generation correctly"
        );
        drop(reader);

        let mut unblock = database.begin().expect("begin unblock");
        unblock.put(b"other", b"final").expect("put unblock");
        unblock.commit().expect("commit unblock");
        database.rebuild_index().expect("fourth rebuild");
        assert_eq!(
            database.get(b"racy").expect("latest read"),
            Some(b"second".to_vec())
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn reclaimed_version_pages_are_freed_and_reused() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut first = database.begin().expect("begin first");
            first.put(b"hot", b"v1").expect("put v1");
            first.commit().expect("commit v1");
            let mut second = database.begin().expect("begin second");
            second.put(b"hot", b"v2").expect("put v2");
            second.commit().expect("commit v2");

            database.rebuild_index().expect("rebuild");
            let stats = database.stats().expect("stats");
            assert!(
                stats.free_pages >= 1,
                "the drained old version's page must be free: {stats:?}"
            );

            let free_before = stats.free_pages;
            let mut third = database.begin().expect("begin third");
            third.put(b"hot", b"v3").expect("put v3");
            third.commit().expect("commit v3");
            let stats = database.stats().expect("stats");
            assert!(
                stats.free_pages < free_before,
                "the new version must have reused a freed page: {stats:?}"
            );
            assert_eq!(database.get(b"hot").expect("get"), Some(b"v3".to_vec()));
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        assert_eq!(
            database.get(b"hot").expect("get after restart"),
            Some(b"v3".to_vec()),
            "reuse must survive a restart"
        );
        database.shutdown().expect("shutdown reopened");
    }

    #[test]
    fn restart_refrees_stale_pages_the_crash_left_behind() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut first = database.begin().expect("begin first");
            first.put(b"key", b"old").expect("put old");
            first.commit().expect("commit old");
            let mut second = database.begin().expect("begin second");
            second.put(b"key", b"new").expect("put new");
            second.commit().expect("commit new");
            database
                .rebuild_index()
                .expect("rebuild frees the old page");
            assert!(database.stats().expect("stats").free_pages >= 1);
            // Shut down without reusing the freed page, like a quiet crash.
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        let stats = database.stats().expect("stats");
        assert!(
            stats.free_pages >= 1,
            "the open sweep must rediscover the stale page as free: {stats:?}"
        );
        assert_eq!(database.get(b"key").expect("get"), Some(b"new".to_vec()));
        database.shutdown().expect("shutdown reopened");
    }

    #[test]
    fn protected_scans_conflict_with_phantom_inserts() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut seed = database.begin().expect("begin seed");
        seed.put(b"acct/1", b"100").expect("put");
        seed.commit().expect("commit seed");

        let mut first = database.begin().expect("begin first");
        first.protect_scans();
        let mut second = database.begin().expect("begin second");
        second.protect_scans();

        assert_eq!(first.scan_prefix(b"acct/").expect("scan").len(), 1);
        assert_eq!(second.scan_prefix(b"acct/").expect("scan").len(), 1);

        first.put(b"acct/2", b"from first").expect("put first");
        second.put(b"acct/3", b"from second").expect("put second");

        first.commit().expect("the first committer wins");
        assert!(
            matches!(
                second.commit(),
                Err(TransactionError::RangeConflict { prefix }) if prefix == b"acct/"
            ),
            "the second writer saw a range that changed under it"
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn unprotected_scans_keep_plain_snapshot_isolation() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());

        let mut first = database.begin().expect("begin first");
        let mut second = database.begin().expect("begin second");
        assert_eq!(first.scan_prefix(b"acct/").expect("scan").len(), 0);
        assert_eq!(second.scan_prefix(b"acct/").expect("scan").len(), 0);
        first.put(b"acct/1", b"one").expect("put first");
        second.put(b"acct/2", b"two").expect("put second");

        first.commit().expect("first commits");
        second
            .commit()
            .expect("plain snapshot isolation admits the phantom");
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn protected_scans_ignore_writes_outside_their_range() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());

        let mut protected = database.begin().expect("begin protected");
        protected.protect_scans();
        assert_eq!(protected.scan_prefix(b"acct/").expect("scan").len(), 0);
        protected.put(b"acct/1", b"one").expect("put");

        let mut unrelated = database.begin().expect("begin unrelated");
        unrelated.put(b"other/9", b"noise").expect("put unrelated");
        unrelated.commit().expect("commit unrelated");

        protected
            .commit()
            .expect("writes outside the scanned range never conflict");
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn sequential_model_matches_committed_database_state() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let mut model = HashMap::<Vec<u8>, Vec<u8>>::new();
        let mut random = 0x4d59_5df4_d0f3_3173_u64;

        for step in 0..500_u64 {
            random = random
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1);
            let key = format!("key:{:02}", random % 31).into_bytes();
            let mut transaction = database.begin().expect("begin");
            if random & 3 == 0 {
                transaction.delete(key.clone()).expect("delete");
                if random & 16 == 0 {
                    transaction.rollback().expect("rollback");
                } else {
                    transaction.commit().expect("commit");
                    model.remove(&key);
                }
            } else {
                let value = format!("value:{step}:{random}").into_bytes();
                transaction.put(key.clone(), value.clone()).expect("put");
                if random & 16 == 0 {
                    transaction.rollback().expect("rollback");
                } else {
                    transaction.commit().expect("commit");
                    model.insert(key, value);
                }
            }
        }

        for key_number in 0..31 {
            let key = format!("key:{key_number:02}").into_bytes();
            assert_eq!(database.get(&key).expect("get"), model.get(&key).cloned());
        }
    }

    #[test]
    fn dropped_and_rolled_back_transactions_leave_no_active_snapshot() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        {
            let _dropped = database.begin().expect("begin dropped transaction");
            assert_eq!(database.stats().expect("stats").active_transactions, 1);
        }
        assert_eq!(database.stats().expect("stats").active_transactions, 0);

        database
            .begin()
            .expect("begin rollback")
            .rollback()
            .expect("rollback");
        assert_eq!(database.stats().expect("stats").active_transactions, 0);
    }

    #[test]
    fn prepared_write_intents_reject_overlapping_writers() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let first = database.begin().expect("first");
        let second = database.begin().expect("second");

        let first_timestamp = database
            .inner
            .prepare_commit(
                first.id,
                first.snapshot,
                [b"same".to_vec()].into_iter(),
                &[],
            )
            .expect("prepare first");
        assert!(matches!(
            database.inner.prepare_commit(
                second.id,
                second.snapshot,
                [b"same".to_vec()].into_iter(),
                &[]
            ),
            Err(TransactionError::WriteConflict { .. })
        ));
        database
            .inner
            .finish_prepared(first.id, first_timestamp, &[(b"same".to_vec(), None)], None)
            .expect("abort prepared commit");
        assert_eq!(database.stats().expect("stats").write_intents, 0);
    }

    #[test]
    fn visibility_waits_for_earlier_commit_timestamps() {
        let directory = tempdir().expect("tempdir");
        let database = open_database(directory.path());
        let first = database.begin().expect("first");
        let second = database.begin().expect("second");
        let first_timestamp = database
            .inner
            .prepare_commit(first.id, first.snapshot, [b"a".to_vec()].into_iter(), &[])
            .expect("prepare first");
        let second_timestamp = database
            .inner
            .prepare_commit(second.id, second.snapshot, [b"b".to_vec()].into_iter(), &[])
            .expect("prepare second");

        database
            .inner
            .finish_prepared(second.id, second_timestamp, &[(b"b".to_vec(), None)], None)
            .expect("finish second");
        assert_eq!(
            database.stats().expect("stats").visible_through,
            Timestamp(0)
        );

        database
            .inner
            .finish_prepared(first.id, first_timestamp, &[(b"a".to_vec(), None)], None)
            .expect("finish first");
        assert_eq!(
            database.stats().expect("stats").visible_through,
            Timestamp(2)
        );
    }

    #[test]
    fn persistent_index_generation_serves_latest_values_after_restart() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut transaction = database.begin().expect("begin");
            for number in 0..1_500_u64 {
                transaction
                    .put(
                        format!("key:{number:08}").into_bytes(),
                        format!("value:{number}").into_bytes(),
                    )
                    .expect("put");
            }
            transaction.commit().expect("commit");

            let generation = database.rebuild_index().expect("rebuild index");
            assert_eq!(generation.timestamp, Timestamp(1));
            assert!(
                generation.root.expect("nonempty root").height >= 2,
                "{generation:?}"
            );
            let stats = database.stats().expect("stats");
            assert_eq!(stats.indexed_through, Some(Timestamp(1)));
            assert_eq!(stats.indexed_keys, 1_500);
            assert_eq!(
                database.get(b"key:00000777").expect("indexed get"),
                Some(b"value:777".to_vec())
            );
            assert_eq!(
                database
                    .begin()
                    .expect("begin indexed scan")
                    .scan_prefix(b"key:0000001")
                    .expect("indexed prefix scan")
                    .len(),
                10
            );
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        let stats = database.stats().expect("stats");
        assert_eq!(stats.indexed_through, Some(Timestamp(1)));
        assert_eq!(stats.indexed_keys, 1_500);
        assert_eq!(
            database.get(b"key:00001499").expect("indexed get"),
            Some(b"value:1499".to_vec())
        );
        assert_eq!(database.get(b"absent").expect("indexed miss"), None);
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn stale_index_generation_never_hides_a_newer_commit() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut initial = database.begin().expect("begin initial");
            initial.put(b"key", b"old").expect("put initial");
            initial.commit().expect("commit initial");
            database.rebuild_index().expect("rebuild index");

            let mut update = database.begin().expect("begin update");
            update.put(b"key", b"new").expect("put update");
            update.commit().expect("commit update");
            assert_eq!(
                database.stats().expect("stats").indexed_through,
                Some(Timestamp(1))
            );
            assert_eq!(database.get(b"key").expect("get"), Some(b"new".to_vec()));
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        assert_eq!(
            database.stats().expect("stats").indexed_through,
            Some(Timestamp(1))
        );
        assert_eq!(database.get(b"key").expect("get"), Some(b"new".to_vec()));
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn empty_index_generation_is_durable() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            assert_eq!(database.checkpoint().expect("checkpoint"), Timestamp(0));
            assert_eq!(
                database.stats().expect("stats").indexed_through,
                Some(Timestamp(0))
            );
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        let stats = database.stats().expect("stats");
        assert_eq!(stats.indexed_through, Some(Timestamp(0)));
        assert_eq!(stats.indexed_keys, 0);
        assert_eq!(database.get(b"missing").expect("get"), None);
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn checkpoint_updates_only_copy_on_write_index_paths() {
        let directory = tempdir().expect("tempdir");
        {
            let database = open_database(directory.path());
            let mut initial = database.begin().expect("begin initial");
            for number in 0..2_000_u64 {
                initial
                    .put(
                        format!("key:{number:08}").into_bytes(),
                        number.to_le_bytes(),
                    )
                    .expect("put initial");
            }
            initial.commit().expect("commit initial");
            database.rebuild_index().expect("initial index");
            let pages_before = database.stats().expect("stats").group_commit.pages;

            let mut delta = database.begin().expect("begin delta");
            delta
                .put(b"key:00001000", b"updated")
                .expect("update indexed key");
            delta.delete(b"key:00001001").expect("delete indexed key");
            delta
                .put(b"key:00001000:after", b"inserted")
                .expect("insert indexed key");
            delta.commit().expect("commit delta");
            database.checkpoint().expect("incremental checkpoint");

            let stats = database.stats().expect("stats");
            let incremental_pages = stats.group_commit.pages - pages_before - 3;
            assert!(
                incremental_pages <= 8,
                "incremental index wrote {incremental_pages} pages: {stats:?}"
            );
            assert_eq!(stats.indexed_through, Some(Timestamp(2)));
            assert_eq!(stats.indexed_keys, 2_000);
            assert_eq!(
                database.get(b"key:00001000").expect("updated get"),
                Some(b"updated".to_vec())
            );
            assert_eq!(database.get(b"key:00001001").expect("deleted get"), None);
            database.shutdown().expect("shutdown");
        }

        let database = open_database(directory.path());
        assert_eq!(
            database.stats().expect("stats").indexed_through,
            Some(Timestamp(2))
        );
        assert_eq!(
            database.get(b"key:00001000:after").expect("inserted get"),
            Some(b"inserted".to_vec())
        );
        database.shutdown().expect("shutdown");
    }

    #[test]
    fn concurrent_commit_and_checkpoint_publish_only_safe_generations() {
        let directory = tempdir().expect("tempdir");
        let database = Arc::new(open_database(directory.path()));
        let mut initial = database.begin().expect("begin initial");
        initial.put(b"key", b"initial").expect("put initial");
        initial.commit().expect("commit initial");
        database.rebuild_index().expect("initial index");

        let mut update = database.begin().expect("begin update");
        update.put(b"key", b"concurrent").expect("put update");
        let barrier = Arc::new(Barrier::new(3));
        let commit_barrier = Arc::clone(&barrier);
        let commit = thread::spawn(move || {
            commit_barrier.wait();
            update.commit().expect("concurrent commit")
        });
        let checkpoint_database = Arc::clone(&database);
        let checkpoint_barrier = Arc::clone(&barrier);
        let checkpoint = thread::spawn(move || {
            checkpoint_barrier.wait();
            checkpoint_database
                .checkpoint()
                .expect("concurrent checkpoint")
        });
        barrier.wait();
        commit.join().expect("join commit");
        let checkpoint_timestamp = checkpoint.join().expect("join checkpoint");
        assert!(
            checkpoint_timestamp == Timestamp(1) || checkpoint_timestamp == Timestamp(2),
            "{checkpoint_timestamp:?}"
        );
        assert_eq!(
            database.get(b"key").expect("get latest"),
            Some(b"concurrent".to_vec())
        );

        assert_eq!(
            database.checkpoint().expect("final checkpoint"),
            Timestamp(2)
        );
        assert_eq!(
            database.stats().expect("stats").indexed_through,
            Some(Timestamp(2))
        );
        let database = match Arc::try_unwrap(database) {
            Ok(database) => database,
            Err(_) => panic!("all database references should be released"),
        };
        database.shutdown().expect("shutdown");

        let reopened = open_database(directory.path());
        assert_eq!(
            reopened.get(b"key").expect("reopened get"),
            Some(b"concurrent".to_vec())
        );
        assert_eq!(
            reopened.stats().expect("stats").indexed_through,
            Some(Timestamp(2))
        );
        reopened.shutdown().expect("shutdown reopened");
    }
}
