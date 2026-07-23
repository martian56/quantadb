# QuantaDB

QuantaDB is an experimental relational database being rebuilt in Rust around
three non-negotiable goals:

1. correctness under concurrency and crashes;
2. predictable low-tail latency for durable OLTP workloads;
3. reproducible, honest performance measurements.

The old v0.1 proof of concept remains available in Git history. The current
`0.2.0` codebase is a clean foundation and is **not yet a database you should
use for data storage**.

## Current milestone

The repository currently contains:

- `syntax/` — a dependency-light, span-aware SQL lexer, AST, and parser;
- `storage/` — checksummed pages, physical WAL, recovery, and bounded caching;
- `index/` — persistent immutable B+ tree generations for point/range access;
- `mvcc/` — durable snapshot transactions with first-committer-wins conflicts;
- `engine/` — durable relational catalog, constraints, and transactional CRUD;
- `server/` — a bounded, concurrent, versioned TCP protocol server;
- `docs/` — syntax, protocol, and architecture documentation.

The server exposes health, SQL parsing, and transactional SQL execution.
Connection-scoped `BEGIN`/`COMMIT` state is isolated across clients and all
engine work is admitted through a bounded blocking-work pool.

The current development focus is finishing M3: online generation publication,
version reclamation, and stronger concurrent history checking.

## Build and test

Rust 1.85 or newer is required.

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```

Start the server:

```bash
cargo run -p quantadb-server
```

It listens on `127.0.0.1:54321` by default.

## Protocol example

Each protocol frame is one UTF-8 JSON object followed by a newline:

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
