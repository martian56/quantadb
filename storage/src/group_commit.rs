use crate::{
    store::{BackgroundCheckpoint, PageAllocator, SharedReader},
    DurableStore, Lsn, Page, PageId, PageWrite, Result, StorageError, MAX_PAGE_PAYLOAD,
};
use std::{
    fs::File,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GroupCommitOptions {
    pub queue_depth: usize,
    pub max_batch_pages: usize,
    pub max_delay: Duration,
    /// Checkpoint and truncate the WAL once it grows past this size.
    ///
    /// Zero disables automatic checkpoints; explicit `checkpoint` calls
    /// still work. The check runs after each committed batch, so the log
    /// can overshoot by at most one batch.
    pub checkpoint_after_wal_bytes: u64,
    /// Dirty pages written to the data file, unsynced, after each batch.
    ///
    /// This paces checkpoint work into small slices between commits so the
    /// checkpoint itself mostly syncs instead of bursting. Zero disables
    /// pacing and checkpoints do all the writing themselves.
    pub flush_pages_per_batch: usize,
}

impl Default for GroupCommitOptions {
    fn default() -> Self {
        Self {
            queue_depth: 1_024,
            max_batch_pages: 256,
            max_delay: Duration::from_micros(200),
            checkpoint_after_wal_bytes: 64 << 20,
            flush_pages_per_batch: 32,
        }
    }
}

impl GroupCommitOptions {
    fn validate(self) -> Result<Self> {
        if self.queue_depth == 0 {
            return Err(StorageError::Configuration(
                "group commit queue depth must be greater than zero".to_owned(),
            ));
        }
        if self.max_batch_pages == 0 {
            return Err(StorageError::Configuration(
                "group commit batch size must be greater than zero".to_owned(),
            ));
        }
        if self.max_delay.is_zero() {
            return Err(StorageError::Configuration(
                "group commit delay must be greater than zero".to_owned(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GroupCommitStats {
    pub groups: u64,
    pub requests: u64,
    pub pages: u64,
    pub automatic_checkpoints: u64,
}

#[derive(Default)]
struct AtomicStats {
    groups: AtomicU64,
    requests: AtomicU64,
    pages: AtomicU64,
    automatic_checkpoints: AtomicU64,
}

impl AtomicStats {
    fn snapshot(&self) -> GroupCommitStats {
        GroupCommitStats {
            groups: self.groups.load(Ordering::Relaxed),
            requests: self.requests.load(Ordering::Relaxed),
            pages: self.pages.load(Ordering::Relaxed),
            automatic_checkpoints: self.automatic_checkpoints.load(Ordering::Relaxed),
        }
    }
}

struct CommitRequest {
    writes: Vec<PageWrite>,
    durable: bool,
    response: SyncSender<Result<Vec<Lsn>>>,
}

enum Command {
    Commit(CommitRequest),
    Checkpoint(SyncSender<Result<Lsn>>),
    Shutdown(SyncSender<()>),
}

pub struct GroupCommitter {
    handle: GroupCommitHandle,
    worker: Option<JoinHandle<()>>,
    checkpointer: Option<JoinHandle<()>>,
}

#[derive(Clone)]
pub struct GroupCommitHandle {
    sender: SyncSender<Command>,
    reader: Arc<SharedReader>,
    allocator: Arc<PageAllocator>,
    stats: Arc<AtomicStats>,
}

impl GroupCommitter {
    pub fn start(store: DurableStore, options: GroupCommitOptions) -> Result<Self> {
        let options = options.validate()?;
        let reader = Arc::new(store.shared_reader()?);
        let allocator = store.page_allocator();
        let data_writer = store.shared_data_writer()?;
        let store = Arc::new(Mutex::new(store));
        let stats = Arc::new(AtomicStats::default());
        let (sender, receiver) = mpsc::sync_channel(options.queue_depth);
        let (checkpoint_sender, checkpoint_receiver) = mpsc::sync_channel(1);
        let checkpointer_store = Arc::clone(&store);
        let checkpointer_stats = Arc::clone(&stats);
        let checkpointer = thread::Builder::new()
            .name("quantadb-checkpoint".to_owned())
            .spawn(move || {
                checkpoint_worker(checkpoint_receiver, checkpointer_store, checkpointer_stats);
            })?;
        let worker_store = store;
        let worker_stats = Arc::clone(&stats);
        let worker = thread::Builder::new()
            .name("quantadb-group-commit".to_owned())
            .spawn(move || {
                commit_worker(
                    receiver,
                    worker_store,
                    worker_stats,
                    options,
                    checkpoint_sender,
                    data_writer,
                );
            })?;

        Ok(Self {
            handle: GroupCommitHandle {
                sender,
                reader,
                allocator,
                stats,
            },
            worker: Some(worker),
            checkpointer: Some(checkpointer),
        })
    }

    #[must_use]
    pub fn handle(&self) -> GroupCommitHandle {
        self.handle.clone()
    }

    pub fn shutdown(mut self) -> Result<()> {
        self.stop_worker()
    }

    fn stop_worker(&mut self) -> Result<()> {
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        let (sender, receiver) = mpsc::sync_channel(0);
        self.handle
            .sender
            .send(Command::Shutdown(sender))
            .map_err(|_| StorageError::CommitCoordinatorStopped)?;
        receiver
            .recv()
            .map_err(|_| StorageError::CommitCoordinatorStopped)?;
        worker
            .join()
            .map_err(|_| StorageError::GroupCommit("worker thread panicked".to_owned()))?;
        // the worker's exit drops the only checkpoint sender
        if let Some(checkpointer) = self.checkpointer.take() {
            checkpointer
                .join()
                .map_err(|_| StorageError::GroupCommit("checkpoint thread panicked".to_owned()))?;
        }
        Ok(())
    }
}

impl Drop for GroupCommitter {
    fn drop(&mut self) {
        let _ = self.stop_worker();
    }
}

impl GroupCommitHandle {
    pub fn commit(&self, writes: Vec<PageWrite>) -> Result<Vec<Lsn>> {
        self.commit_with(writes, true)
    }

    /// Commit a batch that rides the next sync instead of forcing one.
    ///
    /// The pages become visible to readers immediately and durable when
    /// any later synced batch or checkpoint lands. A crash before then
    /// loses the whole batch atomically, so this lane is only for work
    /// the caller can rebuild, like index generations.
    pub fn commit_relaxed(&self, writes: Vec<PageWrite>) -> Result<Vec<Lsn>> {
        self.commit_with(writes, false)
    }

    fn commit_with(&self, writes: Vec<PageWrite>, durable: bool) -> Result<Vec<Lsn>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        for write in &writes {
            if write.payload.len() > MAX_PAGE_PAYLOAD {
                return Err(StorageError::PageTooLarge {
                    actual: write.payload.len(),
                    maximum: MAX_PAGE_PAYLOAD,
                });
            }
        }

        let (sender, receiver) = mpsc::sync_channel(0);
        self.sender
            .send(Command::Commit(CommitRequest {
                writes,
                durable,
                response: sender,
            }))
            .map_err(|_| StorageError::CommitCoordinatorStopped)?;
        receiver
            .recv()
            .map_err(|_| StorageError::CommitCoordinatorStopped)?
    }

    /// Read a page without touching the store mutex.
    ///
    /// Commits hold that mutex across their WAL sync; readers coming
    /// through here check the dirty table and the data file directly, so a
    /// read never waits milliseconds for someone else's fsync.
    pub fn read_page(&self, page_id: PageId) -> Result<Option<Page>> {
        self.reader.read_page(page_id)
    }

    /// Reserve page IDs without touching the store mutex.
    ///
    /// Committers call this while another batch may be mid-sync; going
    /// through the shared allocator keeps them preparing their own batch
    /// instead of standing in line behind someone else's fsync.
    pub fn reserve_page_ids(&self, count: usize) -> Result<Vec<PageId>> {
        self.allocator.reserve(count)
    }

    /// Return unreachable pages to the free pool.
    pub fn release_pages(&self, pages: impl IntoIterator<Item = PageId>) -> Result<()> {
        self.allocator.release(pages)
    }

    pub fn free_page_count(&self) -> Result<usize> {
        self.allocator.free_page_count()
    }

    pub fn checkpoint(&self) -> Result<Lsn> {
        let (sender, receiver) = mpsc::sync_channel(0);
        self.sender
            .send(Command::Checkpoint(sender))
            .map_err(|_| StorageError::CommitCoordinatorStopped)?;
        receiver
            .recv()
            .map_err(|_| StorageError::CommitCoordinatorStopped)?
    }

    #[must_use]
    pub fn stats(&self) -> GroupCommitStats {
        self.stats.snapshot()
    }
}

fn commit_worker(
    receiver: Receiver<Command>,
    store: Arc<Mutex<DurableStore>>,
    stats: Arc<AtomicStats>,
    options: GroupCommitOptions,
    checkpoint_sender: SyncSender<BackgroundCheckpoint>,
    data_writer: File,
) {
    let mut pending = None;
    loop {
        let command = match pending.take() {
            Some(command) => command,
            None => match receiver.recv() {
                Ok(command) => command,
                Err(_) => return,
            },
        };

        match command {
            Command::Commit(first) => {
                let mut requests = vec![first];
                let mut page_count = requests[0].writes.len();
                let deadline = Instant::now() + options.max_delay;

                while page_count < options.max_batch_pages {
                    let remaining = deadline.saturating_duration_since(Instant::now());
                    if remaining.is_zero() {
                        break;
                    }
                    match receiver.recv_timeout(remaining) {
                        Ok(Command::Commit(request)) => {
                            page_count = page_count.saturating_add(request.writes.len());
                            requests.push(request);
                        }
                        Ok(other) => {
                            pending = Some(other);
                            break;
                        }
                        Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => break,
                    }
                }
                execute_group(&store, &stats, requests);
                pace_flush(&store, &data_writer, &options);
                maybe_checkpoint(&store, &stats, &options, &checkpoint_sender);
            }
            Command::Checkpoint(response) => {
                let _ = response.send(explicit_checkpoint(&store));
            }
            Command::Shutdown(response) => {
                let _ = response.send(());
                return;
            }
        }
    }
}

/// Write a slice of dirty pages with the store lock released.
///
/// Commits hold that lock across their log sync, so doing page writes under
/// it put every flushed page on the commit path. Claiming the batch, writing
/// it through a separate handle, then reporting back keeps the lock held
/// only for the two short bookkeeping steps.
fn pace_flush(store: &Mutex<DurableStore>, data: &File, options: &GroupCommitOptions) {
    if options.flush_pages_per_batch == 0 {
        return;
    }
    let claimed = match store.lock() {
        Ok(mut store) => match store.take_flush_batch(options.flush_pages_per_batch) {
            Ok(claimed) => claimed,
            Err(_) => return,
        },
        Err(_) => return,
    };
    if claimed.is_empty() {
        return;
    }

    let mut written = Vec::with_capacity(claimed.len());
    let mut unwritten = Vec::new();
    for (index, page) in claimed.iter().enumerate() {
        if crate::data_file::write_shared(data, page).is_ok() {
            written.push((page.id(), page.lsn()));
        } else {
            unwritten.extend(claimed[index..].iter().map(Page::id));
            break;
        }
    }

    if let Ok(mut store) = store.lock() {
        store.note_flushed(&written, &unwritten);
    }
}

fn maybe_checkpoint(
    store: &Arc<Mutex<DurableStore>>,
    stats: &AtomicStats,
    options: &GroupCommitOptions,
    checkpoint_sender: &SyncSender<BackgroundCheckpoint>,
) {
    if options.checkpoint_after_wal_bytes == 0 {
        return;
    }
    let Ok(mut locked) = store.lock() else {
        return;
    };
    let oversized = locked
        .wal_size_bytes()
        .is_ok_and(|size| size >= options.checkpoint_after_wal_bytes);
    if !oversized || locked.checkpoint_in_flight() {
        return;
    }
    if locked.can_begin_background_checkpoint() {
        let Ok(job) = locked.begin_background_checkpoint() else {
            return;
        };
        drop(locked);
        if checkpoint_sender.try_send(job).is_err() {
            // checkpointer is gone; leave repair to a classic checkpoint
            if let Ok(mut locked) = store.lock() {
                locked.abort_background_checkpoint();
            }
        }
    } else if locked.checkpoint().is_ok() {
        // a failed background run left the spare unrecycled
        stats.automatic_checkpoints.fetch_add(1, Ordering::Relaxed);
    }
}

fn checkpoint_worker(
    receiver: Receiver<BackgroundCheckpoint>,
    store: Arc<Mutex<DurableStore>>,
    stats: Arc<AtomicStats>,
) {
    while let Ok(job) = receiver.recv() {
        let result = job.run();
        let Ok(mut locked) = store.lock() else {
            return;
        };
        match result {
            Ok(written) => {
                if locked
                    .complete_background_checkpoint(&job, &written)
                    .is_ok()
                {
                    stats.automatic_checkpoints.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(_) => locked.abort_background_checkpoint(),
        }
    }
}

fn explicit_checkpoint(store: &Mutex<DurableStore>) -> Result<Lsn> {
    loop {
        {
            let mut locked = store
                .lock()
                .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))?;
            if !locked.checkpoint_in_flight() {
                return locked.checkpoint();
            }
        }
        thread::sleep(Duration::from_millis(1));
    }
}

fn execute_group(store: &Mutex<DurableStore>, stats: &AtomicStats, requests: Vec<CommitRequest>) {
    let request_count = requests.len();
    let page_count = requests
        .iter()
        .map(|request| request.writes.len())
        .sum::<usize>();
    let boundaries = requests
        .iter()
        .scan(0_usize, |offset, request| {
            let start = *offset;
            *offset = offset.saturating_add(request.writes.len());
            Some((start, *offset))
        })
        .collect::<Vec<_>>();
    let writes = requests
        .iter()
        .flat_map(|request| request.writes.iter().cloned())
        .collect::<Vec<_>>();

    let durable = requests.iter().any(|request| request.durable);
    let result = store
        .lock()
        .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))
        .and_then(|mut store| store.write_pages_with(writes, durable));

    stats.groups.fetch_add(1, Ordering::Relaxed);
    stats
        .requests
        .fetch_add(request_count as u64, Ordering::Relaxed);
    stats.pages.fetch_add(page_count as u64, Ordering::Relaxed);

    match result {
        Ok(lsns) => {
            for (request, (start, end)) in requests.into_iter().zip(boundaries) {
                let _ = request.response.send(Ok(lsns[start..end].to_vec()));
            }
        }
        Err(error) => {
            let message = error.to_string();
            for request in requests {
                let _ = request
                    .response
                    .send(Err(StorageError::GroupCommit(message.clone())));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::StoreOptions;
    use std::sync::Barrier;
    use tempfile::tempdir;

    #[test]
    fn oversized_logs_trigger_automatic_checkpoints() {
        let directory = tempdir().expect("tempdir");
        let store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open store");
        let coordinator = GroupCommitter::start(
            store,
            GroupCommitOptions {
                checkpoint_after_wal_bytes: 4096,
                ..GroupCommitOptions::default()
            },
        )
        .expect("coordinator");
        let handle = coordinator.handle();

        for page_id in 0..8_u64 {
            handle
                .commit(vec![PageWrite {
                    page_id: PageId(page_id),
                    payload: vec![0x2b; 2048],
                }])
                .expect("commit");
        }

        for page_id in 0..8_u64 {
            let page = handle
                .read_page(PageId(page_id))
                .expect("read")
                .expect("page");
            assert_eq!(page.payload()[0], 0x2b);
        }
        // background checkpoints report asynchronously, so settle first
        coordinator.shutdown().expect("shutdown");
        let stats = handle.stats();
        assert!(
            stats.automatic_checkpoints > 0,
            "at least one checkpoint must have run: {stats:?}"
        );
    }

    #[test]
    fn commits_keep_landing_while_background_checkpoints_run() {
        let directory = tempdir().expect("tempdir");
        let store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open store");
        let coordinator = GroupCommitter::start(
            store,
            GroupCommitOptions {
                checkpoint_after_wal_bytes: 8192,
                ..GroupCommitOptions::default()
            },
        )
        .expect("coordinator");
        let barrier = Arc::new(Barrier::new(4));
        let mut threads = Vec::new();

        for writer in 0..4_u64 {
            let handle = coordinator.handle();
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                for round in 0..64_u64 {
                    let page_id = PageId(writer * 64 + round);
                    handle
                        .commit(vec![PageWrite {
                            page_id,
                            payload: vec![writer as u8 + 1; 1024],
                        }])
                        .expect("commit under checkpoints");
                }
            }));
        }
        for thread in threads {
            thread.join().expect("writer thread");
        }

        let handle = coordinator.handle();
        let explicit = handle.checkpoint().expect("explicit checkpoint");
        assert!(explicit.0 > 0);
        coordinator.shutdown().expect("shutdown");

        let stats = handle.stats();
        assert!(
            stats.automatic_checkpoints > 0,
            "the tiny threshold must have fired: {stats:?}"
        );
        let reopened =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("reopen");
        let reader = reopened.shared_reader().expect("reader");
        for writer in 0..4_u64 {
            for round in 0..64_u64 {
                let page = reader
                    .read_page(PageId(writer * 64 + round))
                    .expect("read")
                    .expect("page survived");
                assert_eq!(page.payload()[0], writer as u8 + 1);
            }
        }
    }

    #[test]
    fn concurrent_requests_are_combined_into_fewer_sync_groups() {
        let directory = tempdir().expect("tempdir");
        let store =
            DurableStore::open(directory.path(), StoreOptions::default()).expect("open store");
        let coordinator = GroupCommitter::start(
            store,
            GroupCommitOptions {
                queue_depth: 32,
                max_batch_pages: 32,
                max_delay: Duration::from_millis(25),
                ..GroupCommitOptions::default()
            },
        )
        .expect("coordinator");
        let barrier = Arc::new(Barrier::new(9));
        let mut threads = Vec::new();

        for page_id in 0..8_u64 {
            let handle = coordinator.handle();
            let barrier = Arc::clone(&barrier);
            threads.push(thread::spawn(move || {
                barrier.wait();
                handle
                    .commit(vec![PageWrite {
                        page_id: PageId(page_id),
                        payload: page_id.to_le_bytes().to_vec(),
                    }])
                    .expect("commit")
            }));
        }
        barrier.wait();
        for thread in threads {
            assert_eq!(thread.join().expect("thread").len(), 1);
        }

        let handle = coordinator.handle();
        let stats = handle.stats();
        assert_eq!(stats.requests, 8);
        assert_eq!(stats.pages, 8);
        assert!(stats.groups < stats.requests, "{stats:?}");
        for page_id in 0..8_u64 {
            assert_eq!(
                handle
                    .read_page(PageId(page_id))
                    .expect("read")
                    .expect("page")
                    .payload(),
                page_id.to_le_bytes()
            );
        }
        handle.checkpoint().expect("checkpoint barrier");
        coordinator.shutdown().expect("shutdown");
    }
}
