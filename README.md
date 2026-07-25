# QuantaDB

QuantaDB is an experimental relational database being rebuilt in Rust around
three non-negotiable goals:

1. correctness under concurrency and crashes;
2. predictable low-tail latency for durable OLTP workloads;
3. reproducible, honest performance measurements.

The old v0.1 proof of concept remains available in Git history. The current
`0.2.0` codebase is a clean foundation and is **not yet a database you should
use for data storage**.

## What is here

- `syntax/`: a dependency-light, span-aware SQL lexer, AST, and parser
- `storage/`: checksummed pages, a two-segment physical WAL with group
  commit and background checkpointing, recovery, and bounded caching
- `index/`: persistent immutable B+ tree generations for point and range
  access, published online by a background worker
- `mvcc/`: durable snapshot transactions with first-committer-wins
  conflicts; single-statement conflicts are retried on a fresh snapshot
  server side
- `engine/`: durable relational catalog, constraints, and transactional CRUD
- `server/`: a bounded, concurrent, versioned TCP protocol server with a
  PostgreSQL wire listener, so `psql` and stock Postgres drivers work
- `docs/`: syntax, protocol, and architecture documentation

The current focus is write-path performance: keeping checkpoint work off the
commit lane entirely and shrinking what each commit has to log.

## Performance

Every pull request is benched on five machine profiles against the latest
main baseline, and the field (PostgreSQL, MySQL, MariaDB) is measured weekly
on the same runners with one shared driver. The tables below are rewritten
by CI from the newest run on main, so they are current, not curated.

<!-- bench:start -->
Measured on main at `571865042`.

QuantaDB on GitHub runners, loadgen native protocol, 10 seconds per
workload at 4 connections:

| profile | reads ops/s | mixed ops/s | writes ops/s | write p99 ms |
|---|---:|---:|---:|---:|
| linux-arm64-full | 30289 | 13849 | 5598 | 8.9 |
| linux-x64-1cpu-1gb | 20721 | 8129 | 3056 | 15.5 |
| linux-x64-2cpu-2gb | 17663 | 11880 | 6178 | 1.7 |
| linux-x64-full | 25900 | 14135 | 5546 | 3.2 |
| windows-x64-full | 22293 | 5560 | 876 | 16.0 |

The field at 2 CPUs and 2 GiB, identical containers, one shared
driver through each engine's Postgres or MySQL port, measured 2026-07-24:

| engine | reads ops/s | mixed ops/s | writes ops/s |
|---|---:|---:|---:|
| **quantadb** | 22528 | 16774 | 8374 |
| postgres18 | 17060 | 17006 | 11404 |
| mariadb114 | 3735 | 4006 | 6741 |
| mysql84 | 3804 | 3845 | 4847 |

MySQL and MariaDB read throughput is limited by the pure Python client in the shared driver; their write numbers are server bound.
<!-- bench:end -->

Read latencies are the headline: QuantaDB serves reads without touching the
commit path, so read p99 stays sub-millisecond even while writers sync.
Writes are honest work in progress; the gap to Postgres on fast storage is
tracked in the issues, with the measurements attached.

See [the benchmark guide](docs/benchmarks.md) for the methodology, including
why single-profile swings are treated as noise.

## Build and test

Rust 1.96 or newer is required.

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Start the server:

```bash
cargo run -p quantadb-server
```

It listens on `127.0.0.1:54321` for the native protocol and on
`127.0.0.1:55432` for Postgres clients:

```bash
psql "host=127.0.0.1 port=55432 user=quanta dbname=quanta"
```

## Protocol example

Each native protocol frame is one UTF-8 JSON object followed by a newline:

```json
{"protocol_version":1,"request_id":1,"request":{"type":"ping"}}
```

```json
{"protocol_version":1,"request_id":2,"request":{"type":"parse","sql":"SELECT id FROM users WHERE active = true"}}
```

The server never sends unsolicited frames. Every response carries the matching
request ID.

## Configuration

| Variable | Default |
|---|---:|
| `QUANTA_LISTEN_ADDRESS` | `127.0.0.1:54321` |
| `QUANTA_PG_LISTEN_ADDRESS` | `127.0.0.1:55432` |
| `QUANTA_HTTP_LISTEN_ADDRESS` | off |
| `QUANTA_DATA_DIR` | `quantadb-data` |
| `QUANTA_MAX_CONNECTIONS` | `1024` |
| `QUANTA_MAX_IN_FLIGHT_REQUESTS` | `256` |
| `QUANTA_MAX_FRAME_BYTES` | `1048576` |
| `QUANTA_IDLE_TIMEOUT_SECS` | `300` |
| `QUANTA_SHUTDOWN_GRACE_SECS` | `5` |

Logging is controlled with the standard `RUST_LOG` environment variable.

## Project direction

The target is a Linux-first, durable, single-node OLTP database with
PostgreSQL-compatible client support. Distributed operation comes after
single-node recovery, transactions, and replication are trustworthy.

See [the architecture roadmap](docs/architecture.md) for milestone boundaries,
[the MVCC design](docs/mvcc.md) for transaction semantics, the
[index format](docs/index-format-v1.md) for persistent tree invariants, and
[the benchmark guide](docs/benchmarks.md) for how performance is measured.
