# QuantaDB index format v1

The `quantadb-index` crate stores immutable B+ tree generations in ordinary
QuantaDB pages. Generation immutability lets point and range readers traverse
without node latches while a replacement generation is built concurrently.

## Node layout

Node payloads use the `QNIX` magic and little-endian integers.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic `QNIX` |
| 4 | 2 | Format version |
| 6 | 1 | Kind: `1` leaf, `2` internal |
| 7 | 1 | Reserved |
| 8 | 4 | Entry/separator count |
| 12 | 2 | Level (`0` for leaves) |
| 14 | 2 | Reserved |
| 16 | 8 | Next leaf or first child page ID |
| 24 | 8 | Reserved |
| 32 | variable | Encoded entries |

A leaf entry is a four-byte key length, key bytes, and an eight-byte value page
ID. An internal entry is a separator encoded in the same way, followed by its
right child page ID. The header's first-child field represents keys below the
first separator. Keys must be strictly increasing inside every node.

Bulk-built leaves contain forward sibling hints. Copy-on-write generations do
not depend on those hints because replacing a leaf would otherwise require
rewriting its predecessor. Range cursors instead retain their internal-node
path and advance through tree edges. Internal separators contain the first key
of their right subtree.

Builders pack variable-length keys up to the physical page payload limit and
add levels until one root remains.

## Atomic publication

Building a generation reserves fresh page IDs and never overwrites an existing
node. The builder can return an uncommitted write plan so MVCC can append a
`QNIR` root manifest to the same atomic storage batch. A root becomes visible
in memory only after the group-commit coordinator confirms that the complete
batch is durable.

The 48-byte root manifest records:

- snapshot commit timestamp;
- optional root page ID;
- tree height;
- live entry count.

An empty snapshot has a manifest but no root. On restart, MVCC selects the
newest manifest no newer than the greatest durable commit timestamp. An older
generation remains safe but is not used for a newer snapshot.

## Reads

Point lookup performs one binary search per level. Range lookup descends once,
then advances an internal-node cursor until the exclusive end key or result
limit. Traversal validates node levels, key ordering, and page-cycle absence.

## Copy-on-write updates

An editor overlays new nodes on an immutable generation. Upserts and deletes
rewrite only the search path, split variable-sized leaves and internal nodes as
needed, and can collapse a single-child root. Applying several mutations may
create intermediate roots in memory; the final plan persists only new pages
reachable from its final root.

MVCC uses these plans at checkpoint time. It derives mutations from versions
newer than the prior manifest, commits the new paths and manifest atomically,
and keeps regular transaction commits free to use the group-commit pipeline.

## Current limitations

- Restart still scans physical pages to discover the newest root manifest and
  reconstruct MVCC version history.
- Old generations are not reclaimed.
- Regular commits do not update the persistent generation immediately; reads
  newer than the last checkpoint use the in-memory version map.
- Values are physical page IDs; covering and composite SQL key encodings will
  be added with the catalog/type layer.
- Prefix compression and tuned fill factors are not implemented.
