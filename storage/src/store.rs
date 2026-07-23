use crate::{
    data_file::DataFile,
    wal::{Wal, WalKind},
    Lsn, Page, PageId, Result, StorageError,
};
use fs2::FileExt;
use std::{
    collections::HashSet,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

const DATA_FILE_NAME: &str = "data.qdb";
const WAL_FILE_NAME: &str = "wal.qdb";
const LOCK_FILE_NAME: &str = "LOCK";
static OPEN_DIRECTORIES: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StoreOptions {
    pub create_if_missing: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageWrite {
    pub page_id: PageId,
    pub payload: Vec<u8>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
        }
    }
}

/// A strictly durable physical page store.
///
/// Each write synchronizes its WAL record before writing and synchronizing the
/// data page. This is intentionally conservative; group commit will batch the
/// same ordering guarantee without weakening it.
pub struct DurableStore {
    root: PathBuf,
    data: DataFile,
    wal: Wal,
    next_page_id: u64,
    /// Released page IDs waiting for reuse.
    ///
    /// Purely in memory: a released page keeps its stale content until a
    /// new write lands on it through the normal WAL path, so a crash before
    /// reuse loses nothing and the releasing layer rediscovers the same
    /// garbage on restart.
    free_pages: std::collections::BTreeSet<PageId>,
    poisoned: bool,
    _lock: StoreLock,
}

impl DurableStore {
    pub fn open(path: impl AsRef<Path>, options: StoreOptions) -> Result<Self> {
        let root = path.as_ref().to_path_buf();
        if !root.exists() {
            if options.create_if_missing {
                fs::create_dir_all(&root)?;
            } else {
                return Err(StorageError::Configuration(format!(
                    "storage directory does not exist: {}",
                    root.display()
                )));
            }
        }
        if !root.is_dir() {
            return Err(StorageError::Configuration(format!(
                "storage path is not a directory: {}",
                root.display()
            )));
        }

        let lock = open_lock_file(&root)?;
        let data = DataFile::open(&root.join(DATA_FILE_NAME))?;
        let wal = Wal::open(&root.join(WAL_FILE_NAME))?;
        let mut store = Self {
            root,
            data,
            wal,
            next_page_id: 0,
            free_pages: std::collections::BTreeSet::new(),
            poisoned: false,
            _lock: lock,
        };
        store.recover()?;
        let maximum_page_lsn = store.data.max_lsn()?;
        store.wal.ensure_next_lsn_after(maximum_page_lsn)?;
        store.next_page_id = store.calculate_next_page_id()?;
        store.wal.trim_records_to_last_checkpoint();
        Ok(store)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn allocate_page(&mut self, payload: impl Into<Vec<u8>>) -> Result<PageId> {
        let page_id = PageId(self.next_page_id);
        let following_page_id = self
            .next_page_id
            .checked_add(1)
            .ok_or_else(|| StorageError::Configuration("page ID space exhausted".to_owned()))?;
        self.write_page(page_id, payload)?;
        self.next_page_id = self.next_page_id.max(following_page_id);
        Ok(page_id)
    }

    pub fn write_page(&mut self, page_id: PageId, payload: impl Into<Vec<u8>>) -> Result<Lsn> {
        let mut lsns = self.write_pages([PageWrite {
            page_id,
            payload: payload.into(),
        }])?;
        lsns.pop().ok_or_else(|| {
            StorageError::Configuration("single-page write produced no LSN".to_owned())
        })
    }

    /// Durably write a group with one WAL sync and one data-file sync.
    ///
    /// All writes are validated before the WAL is changed. Their WAL records
    /// are then appended and synchronized as a group before any data page is
    /// written, preserving the write-ahead rule.
    pub fn write_pages(&mut self, writes: impl IntoIterator<Item = PageWrite>) -> Result<Vec<Lsn>> {
        if self.poisoned {
            return Err(StorageError::Poisoned);
        }
        let writes: Vec<PageWrite> = writes.into_iter().collect();
        for write in &writes {
            Page::new(write.page_id, write.payload.clone())?;
        }
        if writes.is_empty() {
            return Ok(Vec::new());
        }

        let result = self.write_pages_inner(&writes);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub fn read_page(&mut self, page_id: PageId) -> Result<Option<Page>> {
        self.data.read(page_id)
    }

    pub fn page_count(&self) -> Result<u64> {
        self.data.page_count()
    }

    /// Reserve contiguous IDs without writing pages.
    ///
    /// Reservations are process-local until pages are committed. Abandoned
    /// reservations may leave harmless gaps, which can be reused after restart.
    pub fn reserve_page_ids(&mut self, count: usize) -> Result<Vec<PageId>> {
        if self.poisoned {
            return Err(StorageError::Poisoned);
        }
        let mut page_ids = Vec::with_capacity(count);
        while page_ids.len() < count {
            let Some(reused) = self.free_pages.pop_first() else {
                break;
            };
            page_ids.push(reused);
        }
        let fresh = u64::try_from(count - page_ids.len()).map_err(|_| {
            StorageError::Configuration("page reservation count exceeds u64".to_owned())
        })?;
        let end = self
            .next_page_id
            .checked_add(fresh)
            .ok_or_else(|| StorageError::Configuration("page ID space exhausted".to_owned()))?;
        page_ids.extend((self.next_page_id..end).map(PageId));
        self.next_page_id = end;
        Ok(page_ids)
    }

    /// Return pages to the free pool for reuse by later reservations.
    ///
    /// The caller vouches that nothing can reference these pages anymore.
    /// Their stale content stays on disk until a new write overwrites it,
    /// which keeps the release itself crash-free by construction.
    pub fn release_pages(&mut self, pages: impl IntoIterator<Item = PageId>) {
        for page_id in pages {
            if page_id.0 < self.next_page_id {
                self.free_pages.insert(page_id);
            }
        }
    }

    #[must_use]
    pub fn free_page_count(&self) -> usize {
        self.free_pages.len()
    }

    /// Establish a recovery boundary after all preceding data pages are synced.
    pub fn checkpoint(&mut self) -> Result<Lsn> {
        if self.poisoned {
            return Err(StorageError::Poisoned);
        }
        let result = self.checkpoint_inner();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn write_pages_inner(&mut self, writes: &[PageWrite]) -> Result<Vec<Lsn>> {
        let mut lsns = Vec::with_capacity(writes.len());
        for write in writes {
            lsns.push(self.wal.append_page(write.page_id, &write.payload)?);
        }
        self.wal.append_batch_commit(writes.len())?;
        self.wal.sync()?;

        for (write, lsn) in writes.iter().zip(&lsns) {
            let page = Page::with_lsn(write.page_id, *lsn, write.payload.clone())?;
            self.data.write(&page)?;
            self.next_page_id = self.next_page_id.max(write.page_id.0.saturating_add(1));
        }
        self.data.sync()?;
        Ok(lsns)
    }

    fn checkpoint_inner(&mut self) -> Result<Lsn> {
        self.data.sync()?;
        let lsn = self.wal.append_checkpoint()?;
        self.wal.sync()?;
        self.wal.reset_after_checkpoint()?;
        Ok(lsn)
    }

    /// Current size of the log on disk, which only a checkpoint shrinks.
    pub fn wal_size_bytes(&self) -> Result<u64> {
        self.wal.size_bytes()
    }

    fn recover(&mut self) -> Result<()> {
        let records = committed_page_images(self.wal.records())?;
        let mut wrote_page = false;

        for (lsn, page_id, payload) in records {
            let needs_replay = match self.data.read(page_id) {
                Ok(Some(page)) => page.lsn() < lsn,
                Ok(None) | Err(StorageError::CorruptPage { .. }) => true,
                Err(error) => return Err(error),
            };
            if needs_replay {
                let page = Page::with_lsn(page_id, lsn, payload)?;
                self.data.write(&page)?;
                wrote_page = true;
            }
        }
        if wrote_page {
            self.data.sync()?;
        }
        Ok(())
    }

    fn calculate_next_page_id(&self) -> Result<u64> {
        let data_next = self.data.page_count()?;
        let wal_next = committed_page_images(self.wal.records())?
            .into_iter()
            .map(|(_, page_id, _)| page_id.0.saturating_add(1))
            .max()
            .unwrap_or(0);
        Ok(data_next.max(wal_next))
    }
}

fn committed_page_images(records: &[crate::wal::WalRecord]) -> Result<Vec<(Lsn, PageId, Vec<u8>)>> {
    let start = records
        .iter()
        .rposition(|record| matches!(record.kind, WalKind::Checkpoint))
        .map_or(0, |position| position + 1);
    let mut pending = Vec::new();
    let mut committed = Vec::new();

    for record in &records[start..] {
        match &record.kind {
            WalKind::PageImage { page_id, payload } => {
                pending.push((record.lsn, *page_id, payload.clone()));
            }
            WalKind::BatchCommit { page_count } => {
                let page_count = *page_count as usize;
                if pending.len() < page_count {
                    return Err(StorageError::CorruptWal {
                        offset: 0,
                        reason: format!(
                            "batch commit declares {page_count} pages but only {} are pending",
                            pending.len()
                        ),
                    });
                }
                let committed_start = pending.len() - page_count;
                committed.extend(pending.drain(committed_start..));
                // Any older pending images came from an interrupted batch
                // before this process opened the WAL and are not committed.
                pending.clear();
            }
            WalKind::Checkpoint => {
                return Err(StorageError::CorruptWal {
                    offset: 0,
                    reason: "unexpected checkpoint after recovery boundary".to_owned(),
                });
            }
        }
    }
    // A valid but uncommitted WAL tail is intentionally ignored.
    Ok(committed)
}

struct StoreLock {
    _file: File,
    canonical_root: PathBuf,
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        let registry = OPEN_DIRECTORIES.get_or_init(|| Mutex::new(HashSet::new()));
        match registry.lock() {
            Ok(mut open) => {
                open.remove(&self.canonical_root);
            }
            Err(poisoned) => {
                poisoned.into_inner().remove(&self.canonical_root);
            }
        }
    }
}

fn open_lock_file(root: &Path) -> Result<StoreLock> {
    let canonical_root = root.canonicalize()?;
    let registry = OPEN_DIRECTORIES.get_or_init(|| Mutex::new(HashSet::new()));
    {
        let mut open = registry.lock().map_err(|_| {
            StorageError::Configuration("storage lock registry is poisoned".to_owned())
        })?;
        if !open.insert(canonical_root.clone()) {
            return Err(StorageError::AlreadyOpen(root.to_path_buf()));
        }
    }

    let lock_path = root.join(LOCK_FILE_NAME);
    let lock_result = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(StorageError::from)
        .and_then(|file| match FileExt::try_lock_exclusive(&file) {
            Ok(()) => Ok(StoreLock {
                _file: file,
                canonical_root: canonical_root.clone(),
            }),
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                Err(StorageError::AlreadyOpen(root.to_path_buf()))
            }
            Err(error) => Err(error.into()),
        });

    if lock_result.is_err() {
        let mut open = registry.lock().map_err(|_| {
            StorageError::Configuration("storage lock registry is poisoned".to_owned())
        })?;
        open.remove(&canonical_root);
    }
    lock_result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{page::PAGE_SIZE, Page};
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn durable_pages_survive_reopen() {
        let directory = tempdir().expect("tempdir");
        let page_id;
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            page_id = store
                .allocate_page(b"committed".to_vec())
                .expect("allocate");
        }
        let mut reopened =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("reopen");
        assert_eq!(
            reopened
                .read_page(page_id)
                .expect("read")
                .expect("page")
                .payload(),
            b"committed"
        );
    }

    #[test]
    fn allocation_uses_contiguous_page_ids() {
        let directory = tempdir().expect("tempdir");
        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
        assert_eq!(
            store.allocate_page(b"zero".to_vec()).expect("allocate"),
            PageId(0)
        );
        assert_eq!(
            store.allocate_page(b"one".to_vec()).expect("allocate"),
            PageId(1)
        );
        assert_eq!(
            store.allocate_page(b"two".to_vec()).expect("allocate"),
            PageId(2)
        );
    }

    #[test]
    fn reservation_is_contiguous_and_advances_allocation() {
        let directory = tempdir().expect("tempdir");
        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
        assert_eq!(
            store.reserve_page_ids(3).expect("reserve"),
            vec![PageId(0), PageId(1), PageId(2)]
        );
        assert_eq!(
            store.allocate_page(b"after".to_vec()).expect("allocate"),
            PageId(3)
        );
    }

    #[test]
    fn recovery_replays_a_wal_record_missing_from_the_data_file() {
        let directory = tempdir().expect("tempdir");
        {
            let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
            wal.append_page(PageId(7), b"redo me").expect("append");
            wal.append_batch_commit(1).expect("commit");
            wal.sync().expect("sync WAL");
        }

        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        let page = store
            .read_page(PageId(7))
            .expect("read")
            .expect("recovered page");
        assert_eq!(page.payload(), b"redo me");
        assert_eq!(page.lsn(), Lsn(1));
    }

    #[test]
    fn recovery_repairs_a_corrupt_page_from_valid_wal() {
        let directory = tempdir().expect("tempdir");
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            store
                .write_page(PageId(0), b"recoverable".to_vec())
                .expect("write");
        }
        {
            let data_path = directory.path().join(DATA_FILE_NAME);
            let mut bytes = std::fs::read(&data_path).expect("read data");
            bytes[100] ^= 1;
            std::fs::write(&data_path, bytes).expect("corrupt data");
        }

        let mut recovered =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        assert_eq!(
            recovered
                .read_page(PageId(0))
                .expect("read")
                .expect("page")
                .payload(),
            b"recoverable"
        );
    }

    #[test]
    fn recovery_repairs_sampled_torn_data_page_boundaries() {
        let source_page =
            Page::with_lsn(PageId(0), Lsn(1), b"complete".to_vec()).expect("source page");
        let encoded = source_page.encode();

        for cut in [1_usize, 4, 63, 64, PAGE_SIZE / 2, PAGE_SIZE - 1] {
            let directory = tempdir().expect("tempdir");
            {
                let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
                wal.append_page(PageId(0), b"complete").expect("append");
                wal.append_batch_commit(1).expect("commit");
                wal.sync().expect("sync WAL");
            }
            {
                let mut file = File::create(directory.path().join(DATA_FILE_NAME))
                    .expect("create partial data");
                file.write_all(&encoded[..cut]).expect("write partial page");
                file.sync_data().expect("sync partial page");
            }

            let mut recovered =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
            assert_eq!(
                recovered
                    .read_page(PageId(0))
                    .expect("read")
                    .expect("page")
                    .payload(),
                b"complete",
                "cut at byte {cut}"
            );
        }
    }

    #[test]
    fn recovery_replays_only_records_after_the_last_checkpoint() {
        let directory = tempdir().expect("tempdir");
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            store
                .write_page(PageId(1), b"before".to_vec())
                .expect("write");
            store.checkpoint().expect("checkpoint");
        }
        {
            let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
            wal.ensure_next_lsn_after(1).expect("resume LSNs");
            wal.append_page(PageId(1), b"after").expect("append");
            wal.append_batch_commit(1).expect("commit");
            wal.sync().expect("sync");
        }

        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        assert_eq!(
            store
                .read_page(PageId(1))
                .expect("read")
                .expect("page")
                .payload(),
            b"after"
        );
    }

    #[test]
    fn checkpoints_truncate_the_log_and_recovery_still_works() {
        let directory = tempdir().expect("tempdir");
        let wal_path = directory.path().join(WAL_FILE_NAME);
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            store
                .write_page(PageId(1), b"kept".to_vec())
                .expect("write");
            assert!(
                store.wal_size_bytes().expect("size") > 0,
                "the log must grow before the checkpoint"
            );
            store.checkpoint().expect("checkpoint");
            assert_eq!(
                store.wal_size_bytes().expect("size"),
                0,
                "a checkpoint must leave an empty log"
            );
            store
                .write_page(PageId(2), b"later".to_vec())
                .expect("write after checkpoint");
        }
        assert!(
            std::fs::metadata(&wal_path).expect("metadata").len() > 0,
            "post-checkpoint writes land in the fresh log"
        );

        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("reopen");
        assert_eq!(
            store
                .read_page(PageId(1))
                .expect("read kept")
                .expect("page")
                .payload(),
            b"kept"
        );
        assert_eq!(
            store
                .read_page(PageId(2))
                .expect("read later")
                .expect("page")
                .payload(),
            b"later"
        );
        let third = store.allocate_page(b"fresh".to_vec()).expect("allocate");
        assert_eq!(third, PageId(3), "page IDs continue past the truncation");
    }

    #[test]
    fn released_pages_are_reused_before_fresh_ones() {
        let directory = tempdir().expect("tempdir");
        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
        for fill in 0..3_u8 {
            store.allocate_page(vec![fill; 8]).expect("allocate");
        }
        store.release_pages([PageId(1)]);
        assert_eq!(store.free_page_count(), 1);

        let reserved = store.reserve_page_ids(2).expect("reserve");
        assert_eq!(
            reserved,
            vec![PageId(1), PageId(3)],
            "the released page comes back first, then a fresh one"
        );
        assert_eq!(store.free_page_count(), 0);

        store
            .write_page(PageId(1), b"reused".to_vec())
            .expect("write reused page");
        assert_eq!(
            store
                .read_page(PageId(1))
                .expect("read")
                .expect("page")
                .payload(),
            b"reused"
        );
    }

    #[test]
    fn directory_lock_rejects_a_second_writer() {
        let directory = tempdir().expect("tempdir");
        let _first =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("first open");
        let second = DurableStore::open(directory.path(), StoreOptions::default());
        assert!(matches!(second, Err(StorageError::AlreadyOpen(_))));
    }

    #[test]
    fn grouped_writes_share_sync_boundaries_and_remain_recoverable() {
        let directory = tempdir().expect("tempdir");
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            let lsns = store
                .write_pages([
                    PageWrite {
                        page_id: PageId(10),
                        payload: b"ten".to_vec(),
                    },
                    PageWrite {
                        page_id: PageId(11),
                        payload: b"eleven".to_vec(),
                    },
                ])
                .expect("group write");
            assert_eq!(lsns, vec![Lsn(1), Lsn(2)]);
        }

        let mut reopened =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("reopen");
        assert_eq!(
            reopened
                .read_page(PageId(10))
                .expect("read")
                .expect("page")
                .payload(),
            b"ten"
        );
        assert_eq!(
            reopened
                .read_page(PageId(11))
                .expect("read")
                .expect("page")
                .payload(),
            b"eleven"
        );
    }

    #[test]
    fn repaired_wal_tail_never_reuses_a_durable_page_lsn() {
        let directory = tempdir().expect("tempdir");
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            assert_eq!(
                store
                    .write_page(PageId(1), b"first".to_vec())
                    .expect("write"),
                Lsn(1)
            );
        }
        crate::wal::corrupt_last_record_for_recovery_test(&directory.path().join(WAL_FILE_NAME));

        let mut reopened =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("repair");
        assert_eq!(
            reopened
                .write_page(PageId(1), b"second".to_vec())
                .expect("write after repair"),
            Lsn(2)
        );
    }

    #[test]
    fn recovery_ignores_page_images_without_a_batch_commit() {
        let directory = tempdir().expect("tempdir");
        {
            let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
            wal.append_page(PageId(5), b"uncommitted").expect("append");
            wal.sync().expect("sync uncommitted WAL");
        }

        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        assert_eq!(store.read_page(PageId(5)).expect("read"), None);
    }

    #[test]
    fn recovery_replays_all_pages_of_a_committed_batch() {
        let directory = tempdir().expect("tempdir");
        {
            let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
            wal.append_page(PageId(20), b"twenty").expect("append");
            wal.append_page(PageId(21), b"twenty-one").expect("append");
            wal.append_batch_commit(2).expect("commit");
            wal.sync().expect("sync committed batch");
        }

        let mut store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        assert_eq!(
            store
                .read_page(PageId(20))
                .expect("read")
                .expect("page")
                .payload(),
            b"twenty"
        );
        assert_eq!(
            store
                .read_page(PageId(21))
                .expect("read")
                .expect("page")
                .payload(),
            b"twenty-one"
        );
    }

    #[test]
    fn a_new_commit_does_not_adopt_an_old_uncommitted_tail() {
        let directory = tempdir().expect("tempdir");
        {
            let mut wal = Wal::open(&directory.path().join(WAL_FILE_NAME)).expect("open WAL");
            wal.append_page(PageId(30), b"abandoned").expect("append");
            wal.sync().expect("sync abandoned page");
        }
        {
            let mut store =
                DurableStore::open(directory.path(), StoreOptions::default()).expect("open");
            store
                .write_page(PageId(31), b"committed".to_vec())
                .expect("new commit");
        }

        let mut recovered =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("recover");
        assert_eq!(recovered.read_page(PageId(30)).expect("read"), None);
        assert_eq!(
            recovered
                .read_page(PageId(31))
                .expect("read")
                .expect("page")
                .payload(),
            b"committed"
        );
    }
}
