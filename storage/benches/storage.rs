use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use quantadb_storage::{
    BufferPool, DurableStore, GroupCommitOptions, GroupCommitter, Page, PageId, PageWrite,
    StoreOptions, MAX_PAGE_PAYLOAD,
};
use tempfile::TempDir;

fn payload(fill: u8) -> Vec<u8> {
    vec![fill; MAX_PAGE_PAYLOAD]
}

fn open_store(dir: &TempDir) -> DurableStore {
    DurableStore::open(dir.path(), StoreOptions::default()).expect("store must open")
}

fn page_checksum(c: &mut Criterion) {
    let bytes = payload(0xa5);
    let mut group = c.benchmark_group("page");
    group.throughput(Throughput::Bytes(bytes.len() as u64));
    group.bench_function("new_full_payload", |b| {
        b.iter(|| Page::new(PageId(1), black_box(bytes.clone())).expect("page must encode"));
    });
    group.finish();
}

fn durable_write(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let mut store = open_store(&dir);
    let page_id = store
        .allocate_page(payload(0x00))
        .expect("page must allocate");

    let mut group = c.benchmark_group("store");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.bench_function("durable_write_page", |b| {
        b.iter(|| {
            store
                .write_page(page_id, payload(0x42))
                .expect("write must succeed")
        });
    });
    group.finish();
}

fn group_commit_batch(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let committer = GroupCommitter::start(open_store(&dir), GroupCommitOptions::default())
        .expect("committer must start");
    let handle = committer.handle();
    let page_ids = handle.reserve_page_ids(8).expect("pages must reserve");

    let mut group = c.benchmark_group("store");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    group.bench_function("group_commit_8_pages", |b| {
        b.iter(|| {
            let writes = page_ids
                .iter()
                .map(|page_id| PageWrite {
                    page_id: *page_id,
                    payload: payload(0x17),
                })
                .collect::<Vec<_>>();
            handle.commit(writes).expect("commit must succeed")
        });
    });
    group.finish();
    committer.shutdown().expect("committer must stop");
}

fn buffer_pool_hit(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let mut pool = BufferPool::new(open_store(&dir), 16).expect("pool must open");
    let page_id = pool.allocate(payload(0x33)).expect("page must allocate");

    let mut group = c.benchmark_group("buffer_pool");
    group.bench_function("cached_read", |b| {
        b.iter(|| {
            pool.get(black_box(page_id))
                .expect("read must succeed")
                .expect("page must exist")
                .lsn()
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    page_checksum,
    durable_write,
    group_commit_batch,
    buffer_pool_hit
);
criterion_main!(benches);
