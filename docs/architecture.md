# QuantaDB architecture roadmap

## Product target

QuantaDB targets durable, high-concurrency OLTP on Linux. Performance is
evaluated with fixed hardware, datasets, isolation, and durability settings.
No benchmark result is valid if correctness or durability checks fail.

## Dependency direction

Core crates must form an acyclic dependency graph:

```text
syntax
  ↑
catalog / logical types
  ↑
planner and optimizer
  ↑
execution
  ↑
server

storage ← transactions ← execution
```

Syntax cannot depend on storage, networking, catalogs, or execution. The
server owns framing and connection lifecycle but delegates database operations
to an engine interface.

The server's `RequestService` boundary is synchronous because catalog,
planning, and durable storage operations are synchronous. Each validated
request runs on Tokio's blocking pool behind a separate bounded semaphore.
Network reactor threads therefore never execute storage work, and execution
cannot grow without bound even when the connection limit is high.

## Milestones

### M1: syntax and server foundation

- Span-aware lexer and AST
- DDL and CRUD grammar
- Versioned request/response protocol
- Bounded request frames and connection count
- Idle timeouts and graceful shutdown
- End-to-end protocol tests

### M2: durable storage kernel — foundation implemented

- Fixed-size pages and versioned page format
- Checksums and corruption detection
- Buffer pool with explicit eviction policy
- Write-ahead log with group commit
- Checkpoints and restart recovery
- Deterministic crash/fault-injection tests

Transaction-aware dirty pages and platform-specific I/O optimization remain
follow-up work; their absence does not weaken the current write-ahead and
atomic-batch recovery invariants. The log truncates at checkpoints, which
the commit coordinator triggers automatically on a size budget.

### M3: transactions and indexes — transaction foundation implemented

- MVCC and snapshot isolation — implemented for durable byte keys
- Atomic commit and rollback — implemented
- Concurrent B+ tree indexes — immutable persistent generations and
  copy-on-write checkpoint deltas implemented; per-commit publication remains
- Deadlock/conflict handling — immediate first-committer-wins conflicts
- Model-based concurrency testing — sequential model implemented; concurrent
  history checking remains

The current MVCC map is reconstructed with a physical-page scan at restart.
M3 is not complete until online index publication, range-conflict policy,
version reclamation, and stronger concurrent model checking are implemented.

### M4: execution engine

- Catalog, name binding, and type checking — transactional catalog and initial
  binding/type validation implemented
- Logical and physical plans
- Fast point-query path — primary-key and unique equality paths implemented
- Vectorized scan and join operators — scalar scan executor implemented first
- Cost-based optimization

### M5: production operations

- PostgreSQL wire-protocol compatibility
- Authentication, TLS, and role-based access
- Metrics, tracing, and query diagnostics
- Online backup and point-in-time recovery
- Stable upgrades for data and WAL formats

### M6: replication and distribution

- Replicated log and automated failover
- Read replicas
- Jepsen-style failure testing
- Sharding and rebalancing only after replication is proven

## Engineering gates

Every milestone requires:

- formatting and warning-free Clippy;
- unit, integration, and property tests;
- fuzz targets for untrusted formats;
- benchmark baselines and regression budgets;
- versioned formats and compatibility tests;
- documentation of failure modes.

Unsafe Rust is forbidden by default. Exceptions require a documented safety
argument, dedicated tests, and measured performance evidence.
