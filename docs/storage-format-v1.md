# QuantaDB storage format v1

Storage format v1 is an internal development format. Compatibility is enforced
by explicit magic values and versions; it is not yet promised across QuantaDB
releases.

## Durability invariant

For every page write:

1. validate every page image;
2. append physical page-image records to the WAL;
3. append a batch-commit record containing the page count;
4. synchronize the WAL;
5. write the corresponding data pages;
6. synchronize the data file.

Grouped page writes share steps 4 and 6. A data page is never intentionally
written before its WAL record and batch commit are durable. Recovery ignores
page images without a following commit and replays every page from a committed
batch, preventing prefix commits.

## Data pages

Pages are exactly 8192 bytes. Integer fields are little-endian.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic `QNPG` |
| 4 | 2 | Format version |
| 6 | 2 | Reserved |
| 8 | 8 | Page ID |
| 16 | 8 | Last-applied LSN |
| 24 | 4 | Payload length |
| 28 | 4 | CRC32 |
| 32 | 32 | Reserved |
| 64 | 8128 | Payload and zero padding |

The checksum covers the complete page with the checksum field treated as zero.
Reading verifies magic, version, checksum, payload bounds, and expected page
identity.

## WAL records

WAL records have a 40-byte header followed by a physical page payload.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | Magic `QNWL` |
| 4 | 2 | Format version |
| 6 | 1 | Record type |
| 7 | 1 | Reserved |
| 8 | 4 | Total record length |
| 12 | 4 | Payload length |
| 16 | 8 | Monotonic LSN |
| 24 | 8 | Page ID or `u64::MAX` |
| 32 | 4 | CRC32 |
| 36 | 4 | Reserved |

Record type `1` is a page image. Type `2` commits the immediately preceding
page count stored in its four-byte payload. Type `3` is a checkpoint. Checksums
cover the header, with its checksum field zeroed, and the complete payload.

## Recovery

Opening a store:

1. acquires both an in-process canonical-path lock and an OS file lock;
2. validates the WAL in LSN order;
3. truncates only incomplete final records;
4. locates the last completed checkpoint;
5. replays newer page images when their LSN exceeds the data page LSN;
6. replaces a corrupt target page when a valid newer WAL image exists;
7. synchronizes all replayed pages before opening completes.

Checksum corruption before the WAL tail is fatal. Corruption at or before a
checkpoint remains visible unless a newer WAL image can repair it.

## Current limitations

- Checkpoints do not recycle WAL segments yet.
- Page images are physical rather than physiological records.
- The cache is bounded LRU and write-through; transaction-aware dirty-page
  management and asynchronous flushing remain future work.
- A bounded background coordinator combines concurrent callers into shared
  sync groups. The MVCC layer now adds commit timestamps and publishes versions
  only after the coordinator confirms durability.
- Encryption and compression are not part of format v1.
- MVCC version (`QNMV`), index-node (`QNIX`), and index-root (`QNIR`) payloads
  are independently versioned above the physical page layer.
