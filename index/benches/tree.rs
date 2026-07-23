use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion};
use quantadb_index::{BPlusTree, IndexEntry, IndexMutation, IndexRoot};
use quantadb_storage::{
    GroupCommitHandle, GroupCommitOptions, GroupCommitter, PageId, StoreOptions,
};
use tempfile::TempDir;

const TREE_ENTRIES: u64 = 100_000;

fn key(id: u64) -> Vec<u8> {
    id.to_be_bytes().to_vec()
}

fn built_tree(dir: &TempDir) -> (GroupCommitter, GroupCommitHandle, IndexRoot) {
    let store = quantadb_storage::DurableStore::open(dir.path(), StoreOptions::default())
        .expect("store must open");
    let committer =
        GroupCommitter::start(store, GroupCommitOptions::default()).expect("committer must start");
    let handle = committer.handle();
    let entries = (0..TREE_ENTRIES).map(|id| IndexEntry {
        key: key(id),
        value: PageId(id + 1),
    });
    let root = BPlusTree::build(&handle, entries)
        .expect("build must succeed")
        .expect("tree must have a root");
    (committer, handle, root)
}

fn point_lookup(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let (committer, handle, root) = built_tree(&dir);

    let mut group = c.benchmark_group("tree");
    let mut probe = 0_u64;
    group.bench_function("point_lookup_100k", |b| {
        b.iter(|| {
            probe = (probe + 7919) % TREE_ENTRIES;
            BPlusTree::get(&handle, root, black_box(&key(probe)))
                .expect("lookup must succeed")
                .expect("key must exist")
        });
    });
    group.finish();
    drop(handle);
    committer.shutdown().expect("committer must stop");
}

fn range_scan(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let (committer, handle, root) = built_tree(&dir);

    let mut group = c.benchmark_group("tree");
    group.bench_function("range_scan_100_of_100k", |b| {
        b.iter(|| {
            let start = key(40_000);
            BPlusTree::range(&handle, root, Some(black_box(&start)), None, 100)
                .expect("scan must succeed")
        });
    });
    group.finish();
    drop(handle);
    committer.shutdown().expect("committer must stop");
}

fn copy_on_write_plan(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let (committer, handle, root) = built_tree(&dir);

    let mut group = c.benchmark_group("tree");
    group.bench_function("edit_plan_100_upserts", |b| {
        b.iter(|| {
            let mutations = (0..100_u64).map(|offset| {
                IndexMutation::Upsert(IndexEntry {
                    key: key(offset * 997),
                    value: PageId(offset + 1),
                })
            });
            BPlusTree::edit_plan(&handle, Some(root), mutations).expect("plan must succeed")
        });
    });
    group.finish();
    drop(handle);
    committer.shutdown().expect("committer must stop");
}

criterion_group!(benches, point_lookup, range_scan, copy_on_write_plan);
criterion_main!(benches);
