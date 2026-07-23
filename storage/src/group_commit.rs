use crate::{DurableStore, Lsn, Page, PageId, PageWrite, Result, StorageError, MAX_PAGE_PAYLOAD};
use std::{
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
}

impl Default for GroupCommitOptions {
    fn default() -> Self {
        Self {
            queue_depth: 1_024,
            max_batch_pages: 256,
            max_delay: Duration::from_micros(200),
            checkpoint_after_wal_bytes: 64 << 20,
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
}

#[derive(Clone)]
pub struct GroupCommitHandle {
    sender: SyncSender<Command>,
    store: Arc<Mutex<DurableStore>>,
    stats: Arc<AtomicStats>,
}

impl GroupCommitter {
    pub fn start(store: DurableStore, options: GroupCommitOptions) -> Result<Self> {
        let options = options.validate()?;
        let store = Arc::new(Mutex::new(store));
        let stats = Arc::new(AtomicStats::default());
        let (sender, receiver) = mpsc::sync_channel(options.queue_depth);
        let worker_store = Arc::clone(&store);
        let worker_stats = Arc::clone(&stats);
        let worker = thread::Builder::new()
            .name("quantadb-group-commit".to_owned())
            .spawn(move || commit_worker(receiver, worker_store, worker_stats, options))?;

        Ok(Self {
            handle: GroupCommitHandle {
                sender,
                store,
                stats,
            },
            worker: Some(worker),
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
                response: sender,
            }))
            .map_err(|_| StorageError::CommitCoordinatorStopped)?;
        receiver
            .recv()
            .map_err(|_| StorageError::CommitCoordinatorStopped)?
    }

    pub fn read_page(&self, page_id: PageId) -> Result<Option<Page>> {
        self.store
            .lock()
            .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))?
            .read_page(page_id)
    }

    pub fn reserve_page_ids(&self, count: usize) -> Result<Vec<PageId>> {
        self.store
            .lock()
            .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))?
            .reserve_page_ids(count)
    }

    /// Return unreachable pages to the store's free pool.
    pub fn release_pages(&self, pages: impl IntoIterator<Item = PageId>) -> Result<()> {
        self.store
            .lock()
            .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))?
            .release_pages(pages);
        Ok(())
    }

    pub fn free_page_count(&self) -> Result<usize> {
        Ok(self
            .store
            .lock()
            .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))?
            .free_page_count())
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
                maybe_checkpoint(&store, &stats, &options);
            }
            Command::Checkpoint(response) => {
                let result = store
                    .lock()
                    .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))
                    .and_then(|mut store| store.checkpoint());
                let _ = response.send(result);
            }
            Command::Shutdown(response) => {
                let _ = response.send(());
                return;
            }
        }
    }
}

/// Truncate the log once it outgrows the configured budget.
///
/// Runs on the worker thread between batches, so no commit ever waits on a
/// checkpoint that its own batch triggered. A failed checkpoint poisons the
/// store, which makes the next commit fail loudly instead of silently
/// running with an unbounded log.
fn maybe_checkpoint(
    store: &Mutex<DurableStore>,
    stats: &AtomicStats,
    options: &GroupCommitOptions,
) {
    if options.checkpoint_after_wal_bytes == 0 {
        return;
    }
    let Ok(mut store) = store.lock() else {
        return;
    };
    let oversized = store
        .wal_size_bytes()
        .is_ok_and(|size| size >= options.checkpoint_after_wal_bytes);
    if oversized && store.checkpoint().is_ok() {
        stats.automatic_checkpoints.fetch_add(1, Ordering::Relaxed);
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

    let result = store
        .lock()
        .map_err(|_| StorageError::GroupCommit("store mutex is poisoned".to_owned()))
        .and_then(|mut store| store.write_pages(writes));

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

        let stats = handle.stats();
        assert!(
            stats.automatic_checkpoints > 0,
            "the worker must have checkpointed at least once: {stats:?}"
        );
        for page_id in 0..8_u64 {
            let page = handle
                .read_page(PageId(page_id))
                .expect("read")
                .expect("page");
            assert_eq!(page.payload()[0], 0x2b);
        }
        coordinator.shutdown().expect("shutdown");
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
