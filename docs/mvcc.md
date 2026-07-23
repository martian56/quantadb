# QuantaDB MVCC transactions

The `quantadb-mvcc` crate is the first transaction layer above the durable page
store. It provides snapshot isolation for byte keys while the catalog, SQL
executor, and persistent indexes are still under construction.

## Visibility model

Each transaction captures the database's `visible_through` timestamp when it
begins. Reads select the newest committed version whose timestamp is no newer
than that snapshot. Local writes overlay the snapshot, providing read-your-
writes behavior for point reads and prefix scans.

Commit timestamps are allocated in memory and may finish out of order. A
watermark advances only across consecutive completed timestamps, including
aborted commits. This prevents a new transaction from observing timestamp 2
while timestamp 1 is still waiting for durable storage.

## Commit protocol

For a writing transaction:

1. validate that no newer committed version or foreign write intent exists;
2. reserve a commit timestamp and acquire per-key write intents;
3. encode every immutable version into a newly reserved physical page;
4. submit all pages to the storage group-commit coordinator;
5. wait for the WAL and data syncs to complete;
6. publish the versions in memory, release intents, and advance visibility.

All pages from one transaction are part of one physical WAL batch. The group
commit coordinator can place multiple transactions in the same larger atomic
batch. A crash therefore exposes all or none of that durable batch during
recovery.

Read-only commits allocate no timestamp and perform no storage I/O. Dropping or
rolling back a transaction removes its active snapshot.

## Restart

Version records use the versioned `QNMV` format and carry their key, optional
value, and commit timestamp. Opening the database scans valid MVCC pages,
rebuilds the ordered in-memory version map, and resumes timestamp allocation
after the greatest durable timestamp. Tombstones are retained as versions.

`rebuild_index` creates an immutable B+ tree for one visible snapshot and
atomically stores its root manifest with every new node. After the first bulk
generation, rebuilds walk the set of keys committed since the last manifest
and perform copy-on-write path updates, so publication cost tracks the write
rate rather than the database size. `checkpoint` updates the index generation
before writing the physical checkpoint record.

By default a background publisher rebuilds the generation whenever commits
mark keys dirty, so the persistent index follows the visible timestamp
without checkpoints. Foreground commits never wait on index builds; both go
through the same group-commit coordinator and only share storage batches.
Restart seeds the dirty set from recovered history and catches up
automatically. `MvccOptions::online_index` disables the publisher for
callers that want checkpoint-only generations.

The version map is authoritative for every key it holds. Only keys absent
from the map fall through to the newest generation at or below the read
snapshot, so a stale index can never hide a newer value or resurrect a
deleted one.

## Reclamation

The database keeps a ring of published generations, ascending by timestamp.
Reads pick the newest generation at or below their snapshot whenever the
version map has no version at or below it for a key. That rule is what
makes aggressive reclamation safe: a version can leave memory as long as
some generation an active snapshot can reach still holds it.

After each generation publish, and once at open, a sweep drops what no
active or future snapshot can need. Versions above the oldest active
snapshot always survive; below it only the newest version per key does,
and a key leaves the map entirely once the covering generation, the newest
one at or below the oldest active snapshot, holds its surviving version.
Generations older than the covering one are unreachable and leave the
ring. Keys with pending intents or unpublished commits are never removed.

The version map is therefore a working set over the index rather than the
whole database history: cold keys cost no memory, tombstones disappear once
a generation passes them, and the sweep at open keeps a restart from
resurrecting reclaimed history. An open snapshot pins the history it can
see, so reclamation waits for long readers rather than breaking them, and
a reclaimed key that is later rewritten still serves its old value to old
snapshots through the ring. Version pages on disk are not yet recycled;
that needs free-page tracking in the storage layer.

## Conflict behavior

The current isolation level is snapshot isolation with first-committer-wins
write conflicts. Two overlapping transactions may read the same snapshot, but
only one can commit a write to the same key. Disjoint keys can commit
concurrently and share storage syncs.

There is no lock waiting or deadlock cycle: a conflicting writer fails
immediately. Serializable isolation and predicate/range conflict tracking are
not implemented yet.

## Current limitations

- Restart still discovers manifests and rebuilds version history by scanning
  physical pages.
- Keys and values must fit together in one page payload.
- Reclamation is in-memory only; version pages on disk wait on storage-level
  free-page tracking.
- There is no schema, catalog, secondary index, or SQL execution integration.
- The on-disk MVCC format is internal and has no stable-upgrade promise yet.
