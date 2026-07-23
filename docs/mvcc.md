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
generation, it derives key deltas since the previous manifest and performs
copy-on-write path updates. Point and prefix reads use a generation only when
its timestamp exactly matches their snapshot; a later commit automatically
falls back to the version map, so a stale index cannot hide a newer value.
`checkpoint` updates the index generation before writing the physical
checkpoint record.

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
  physical pages; the persistent B+ tree accelerates matching-snapshot reads.
- Persistent generations advance at checkpoints rather than every commit;
  online publication without serializing the group-commit path remains.
- Keys and values must fit together in one page payload.
- Old versions and tombstones are not reclaimed yet.
- Active snapshots are tracked for future garbage collection but do not yet
  drive reclamation.
- There is no schema, catalog, secondary index, or SQL execution integration.
- The on-disk MVCC format is internal and has no stable-upgrade promise yet.
