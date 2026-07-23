# QuantaDB benchmarks

Performance claims in this project are only worth what their measurements
show. This page documents how to run the benchmarks and records baseline
numbers so regressions are caught in review instead of discovered later.

Two rules apply to every number:

1. A result is invalid if any correctness or durability test fails.
2. A baseline records its hardware, operating system, and settings. Numbers
   from different machines are not comparable.

## Microbenchmarks

Each core crate carries criterion benches for its hot paths:

```bash
cargo bench -p quantadb-syntax
cargo bench -p quantadb-storage
cargo bench -p quantadb-index
cargo bench -p quantadb-mvcc
cargo bench -p quantadb-engine
```

Durable cases (single page writes, group commit, MVCC commit, autocommit
insert) pay real WAL and data syncs on every iteration, so those suites use
short measurement windows. Read benches use a large stride so consecutive
probes do not hit the same cache lines.

## End to end workload

`loadgen` drives a running server over TCP with a mixed point workload and
reports throughput plus latency percentiles, with reads and writes split:

```bash
cargo build --workspace --release
./target/release/quantadb-server
./target/release/loadgen --connections 8 --seconds 10 --read-percent 80 --rows 10000
```

Write conflicts from overlapping autocommit updates are counted separately
from failures. They are expected behavior under first-committer-wins, not
noise to hide.

## Baseline (2026-07-23)

Measured on the development machine; treat it as a reference point, not a
marketing number.

- Windows 11 Pro, Intel Core i5-1035G1, 20 GB RAM, WD SN735 NVMe 256 GB
- `cargo bench` defaults: release profile, no target-cpu flags
- Full WAL and data sync on every commit

Median criterion times:

| Benchmark | Median |
|---|---:|
| parse/point_select | 2.8 us |
| parse/filtered_select | 5.6 us |
| parse/create_table | 5.4 us |
| parse/transaction_script | 17.2 us |
| page/new_full_payload | 158 ns |
| buffer_pool/cached_read | 29 ns |
| store/durable_write_page | 13.3 ms |
| store/group_commit_8_pages | 16.1 ms |
| tree/point_lookup_100k | 117 us |
| tree/range_scan_100_of_100k | 153 us |
| tree/edit_plan_100_upserts | 17.7 ms |
| mvcc/single_key_commit | 16.1 ms |
| mvcc/snapshot_point_read_10k | 583 ns |
| mvcc/prefix_scan_1k | 352 us |
| engine/autocommit_insert | 33.5 ms |
| engine/point_select_10k_rows | 14.0 us |
| engine/filtered_scan_10k_rows | 18.1 ms |

One `loadgen` pass on the same machine (8 connections, 5 s, 80 percent
reads, 5000 rows): 736 ops/s total, read p50 388 us and p99 3.5 ms, write
p50 23 ms and p99 196 ms, 4 conflicts, 0 failures.

What the baseline already says about the code:

- Every durable operation on this disk costs roughly 16 ms of sync time.
  That is the disk, not the code, and it is why write latency percentiles
  and durable microbenches cluster there. A faster disk moves all of them.
- A B+ tree point lookup takes 117 us while an in-memory snapshot read
  takes 583 ns. Every node visit is a round trip through the group commit
  thread with no node caching, which is an obvious place to win.
- The filtered scan takes 18 ms against 14 us for the point path on the
  same 10 thousand rows. That is the gap secondary indexes have to close.
