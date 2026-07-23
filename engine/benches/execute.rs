use std::hint::black_box;
use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion};
use quantadb_engine::DatabaseEngine;
use quantadb_mvcc::MvccOptions;
use tempfile::TempDir;

fn seeded_engine(dir: &TempDir, rows: u64) -> DatabaseEngine {
    let engine =
        DatabaseEngine::open(dir.path(), MvccOptions::default()).expect("engine must open");
    let mut session = engine.session();
    session
        .execute("CREATE TABLE accounts (id BIGINT PRIMARY KEY, name TEXT NOT NULL, balance DOUBLE NOT NULL)")
        .expect("create table must succeed");
    let mut id = 0_u64;
    while id < rows {
        let batch_end = (id + 500).min(rows);
        let values = (id..batch_end)
            .map(|row| format!("({row}, 'account {row}', {row}.5)"))
            .collect::<Vec<_>>()
            .join(", ");
        session
            .execute(&format!(
                "INSERT INTO accounts (id, name, balance) VALUES {values}"
            ))
            .expect("seed insert must succeed");
        id = batch_end;
    }
    engine
}

fn autocommit_insert(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let engine = seeded_engine(&dir, 0);
    let mut session = engine.session();

    let mut group = c.benchmark_group("engine");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(500));
    group.measurement_time(Duration::from_secs(3));
    let mut id = 0_u64;
    group.bench_function("autocommit_insert", |b| {
        b.iter(|| {
            id += 1;
            session
                .execute(&format!(
                    "INSERT INTO accounts (id, name, balance) VALUES ({id}, 'inserted', 1.0)"
                ))
                .expect("insert must succeed")
        });
    });
    group.finish();
}

fn point_select(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let engine = seeded_engine(&dir, 10_000);
    let mut session = engine.session();

    let mut group = c.benchmark_group("engine");
    let mut probe = 0_u64;
    group.bench_function("point_select_10k_rows", |b| {
        b.iter(|| {
            probe = (probe + 7919) % 10_000;
            session
                .execute(black_box(&format!(
                    "SELECT id, name, balance FROM accounts WHERE id = {probe}"
                )))
                .expect("select must succeed")
        });
    });
    group.finish();
}

fn scan_with_predicate(c: &mut Criterion) {
    let dir = TempDir::new().expect("temp dir must exist");
    let engine = seeded_engine(&dir, 10_000);
    let mut session = engine.session();

    let mut group = c.benchmark_group("engine");
    group.bench_function("filtered_scan_10k_rows", |b| {
        b.iter(|| {
            session
                .execute(black_box(
                    "SELECT id FROM accounts WHERE balance > 9990.0 LIMIT 20",
                ))
                .expect("select must succeed")
        });
    });
    group.finish();
}

criterion_group!(benches, autocommit_insert, point_select, scan_with_predicate);
criterion_main!(benches);
