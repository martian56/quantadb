use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use quantadb_mvcc::{MvccDatabase, MvccOptions};
use tempfile::TempDir;

fn key(id: u64) -> Vec<u8> {
    let mut key = b"bench/".to_vec();
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn open_database(dir: &TempDir) -> MvccDatabase {
    MvccDatabase::open(dir.path(), MvccOptions::default()).expect("database must open")
}

fn durable_commit(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let database = open_database(&dir);

    let mut group = c.benchmark_group("mvcc");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    let mut id = 0_u64;
    group.bench_function("single_key_commit", |b| {
        b.iter(|| {
            id += 1;
            let mut txn = database.begin().expect("begin must succeed");
            txn.put(key(id), b"value".to_vec()).expect("put must stage");
            txn.commit().expect("commit must succeed")
        });
    });
    group.finish();
    database.shutdown().expect("database must stop");
}

fn snapshot_read(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let database = open_database(&dir);
    let mut txn = database.begin().expect("begin must succeed");
    for id in 0..10_000_u64 {
        txn.put(key(id), id.to_be_bytes().to_vec())
            .expect("put must stage");
    }
    txn.commit().expect("commit must succeed");

    let mut group = c.benchmark_group("mvcc");
    let mut probe = 0_u64;
    group.bench_function("snapshot_point_read_10k", |b| {
        b.iter(|| {
            probe = (probe + 7919) % 10_000;
            database
                .get(black_box(&key(probe)))
                .expect("read must succeed")
                .expect("key must exist")
        });
    });
    group.finish();
    database.shutdown().expect("database must stop");
}

fn prefix_scan(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let database = open_database(&dir);
    let mut txn = database.begin().expect("begin must succeed");
    for id in 0..1_000_u64 {
        txn.put(key(id), id.to_be_bytes().to_vec())
            .expect("put must stage");
    }
    txn.commit().expect("commit must succeed");

    let mut group = c.benchmark_group("mvcc");
    group.bench_function("prefix_scan_1k", |b| {
        b.iter(|| {
            let txn = database.begin().expect("begin must succeed");
            let rows = txn.scan_prefix(black_box(b"bench/")).expect("scan must succeed");
            txn.rollback().expect("rollback must succeed");
            rows
        });
    });
    group.finish();
    database.shutdown().expect("database must stop");
}

criterion_group!(benches, durable_commit, snapshot_read, prefix_scan);
criterion_main!(benches);
