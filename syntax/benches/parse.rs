use std::hint::black_box;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use quantadb_syntax::parse_sql;

const POINT_SELECT: &str = "SELECT id, name, balance FROM accounts WHERE id = 42";

const FILTERED_SELECT: &str = "SELECT id, name, balance FROM accounts \
    WHERE balance >= 100.0 AND (region = 'eu' OR region = 'us') AND closed IS NULL \
    LIMIT 50";

const CREATE_TABLE: &str = "CREATE TABLE IF NOT EXISTS accounts (\
    id BIGINT PRIMARY KEY, \
    name VARCHAR(120) NOT NULL, \
    region TEXT NOT NULL, \
    balance DOUBLE NOT NULL, \
    closed BOOL)";

const SCRIPT: &str = "BEGIN; \
    INSERT INTO accounts (id, name, region, balance) VALUES \
    (1, 'first', 'eu', 10.5), (2, 'second', 'us', 20.25), (3, 'third', 'eu', 0.0); \
    UPDATE accounts SET balance = balance + 1 WHERE region = 'eu'; \
    DELETE FROM accounts WHERE balance < 0; \
    COMMIT";

fn bench_statement(c: &mut Criterion, name: &str, sql: &str) {
    let mut group = c.benchmark_group("parse");
    group.throughput(Throughput::Bytes(sql.len() as u64));
    group.bench_function(name, |b| {
        b.iter(|| parse_sql(black_box(sql)).expect("benchmark SQL must parse"));
    });
    group.finish();
}

fn parse_benchmarks(c: &mut Criterion) {
    bench_statement(c, "point_select", POINT_SELECT);
    bench_statement(c, "filtered_select", FILTERED_SELECT);
    bench_statement(c, "create_table", CREATE_TABLE);
    bench_statement(c, "transaction_script", SCRIPT);
}

criterion_group!(benches, parse_benchmarks);
criterion_main!(benches);
