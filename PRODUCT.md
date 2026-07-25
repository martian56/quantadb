# QuantaDB product truth

## What it is

An experimental relational OLTP database written in Rust, rebuilt from a
retired v0.1 prototype. Version 0.2.0. Apache-2.0. Not a database anyone
should store real data in yet, and the site says so.

## The mechanism nobody else claims

Every performance number QuantaDB publishes carries the machine it ran on,
the commit that produced it, the container limits, and the runs it lost.
CI rewrites the published tables from the newest run on main, so the numbers
are current rather than curated. A result is discarded if a correctness test
fails.

## Audience and scene

Database and systems engineers evaluating an unfamiliar engine, usually
arriving from a repository link with a few minutes of skeptical attention.
They have read benchmark marketing before and discount it by habit. They
want the method before the number.

## What the first surface must prove

That the numbers are real, reproducible, and include the losses.

## Shape of the system

Six crates with an enforced acyclic graph: `syntax` (span-aware SQL parser),
`storage` (checksummed pages, two-segment WAL, group commit, recovery),
`index` (immutable B+ tree generations published online), `mvcc` (snapshot
isolation, first-committer-wins), `engine` (catalog, constraints,
transactional CRUD), `server` (bounded concurrent TCP server).

## Protocol

One native protocol. JSON frames, one per line, over TCP on port 6626.
Documented in `docs/protocol-v1.md`. The PostgreSQL wire listener was
removed; nothing in the product speaks Postgres any more.

## Constraints that bind the site

- No authentication, authorisation, or TLS on any listener.
- No replication.
- No published container image, so the quickstart builds from source.
- The site is static, deployed from `web/` to GitHub Pages, and loads no
  external resources.

## Brand commitments

- Purple mark, `#7132F5`, already carried by `web/logo.svg` and the favicons.
- Benchmark tables on the site and in the README are written by CI from the
  same run artifacts. They are never edited by hand.
- Standing preference recorded 2026-07-25: the site follows the developer
  infrastructure convention, executed at full fidelity, with Neon, Turso and
  ClickHouse as the craft bar. This was chosen deliberately over two more
  expressive directions and holds until the user says otherwise.

## Known gap the site must not paper over

The published field comparison drove QuantaDB through its PostgreSQL port
with psycopg2, the same driver the real Postgres got. That port no longer
exists. The numbers remain real measurements of the engine, but the run is
not reproducible against the current commit until `.github/workflows/
reference.yml` and `.github/scripts/field_bench.py` are re-pointed at the
native client. Until then the table states the commit it was measured at.
