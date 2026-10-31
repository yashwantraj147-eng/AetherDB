# AetherDB Storage Engine Architecture

## Overview
AetherDB implements a dual-engine architecture:
1. **LSM-Tree Key-Value Engine**: Optimized for high-throughput write-intensive transactional mutations with write-ahead logging, memory buffering in `MemTable`, immutable disk-backed sorted string tables (`SSTable`), and background multi-way compaction.
2. **Columnar Analytics Engine**: Optimized for projection and aggregation queries with typed column blocks, Run-Length Encoding (RLE), Dictionary compression, and Min-Max Zone Maps.

---

## 1. Engine Lifecycle State Machine & Shutdown Protocol

`HybridStorageManager` enforces an explicit 4-state lifecycle:
- `OPEN`: Accepting read, write, flush, and compaction operations.
- `CLOSING`: Shutdown initiated; all new writes and new flushes/compactions are rejected immediately. In-flight operations are allowed to complete up to a bounded timeout.
- `CLOSED`: All background executors and resources have cleanly terminated and closed.
- `FAILED`: Fatal I/O or rotation error occurred; engine rejects new writes to prevent acknowledged data loss while keeping existing readers available.

### Concurrency & Double-Check Under Synchronization:
All mutating operations (`put`, `delete`, `flushMemTable`, `compact`) re-validate the lifecycle state *after* acquiring the write lock. Read operations (`get`) validate state before and after acquiring the read lock, eliminating shutdown races.

### Graceful Bounded Shutdown:
During `close()`:
1. State atomically transitions to `CLOSING`.
2. Active background flushes and compactions are awaited up to a bounded timeout (`executorTerminationTimeoutMs`, default 5 seconds).
3. If tasks do not terminate within the timeout, resources (WAL and active SSTables) are safely left open in `CLOSING` state rather than closed beneath active worker threads, allowing safe retry.
4. If workers finish cleanly, all WAL files and SSTable readers are closed, and state transitions to `CLOSED`.
5. Background tasks cancelled during shutdown verify state and discard/close their uninstalled readers rather than publishing them to the live table collection.

---

## 2. Write Path, Pre-Rotation & Flush Recovery

Every write operation (`put` or `delete`) executes under an explicit ordering protocol:

1. **Write Lock Acquisition & State Validation**: The engine acquires `rwLock.writeLock()` and asserts state is `OPEN`.
2. **Atomic Sequence Generation**: A 64-bit monotonically increasing sequence number is assigned (`sequenceNumberGenerator.getAndIncrement()`).
3. **DataRecord Construction**: Key and value byte arrays are defensively copied to guarantee record immutability.
4. **WAL Append**: The framed record is appended to the active WAL segment (`wal.append(record)`).
   - In `IMMEDIATE` sync mode, `FileChannel.force(false)` is invoked synchronously before proceeding.
   - In `PERIODIC` sync mode, dirty pages are flushed on a background timer (default 50ms).
5. **MemTable Update**: The record is inserted into the active `MemTable`. The `ConcurrentSkipListMap` enforces version monotonicity: an older sequence number will never overwrite a newer sequence number for the same key.
6. **Flush Threshold & Pre-Rotation**: If active `MemTable` memory footprint exceeds `lsm.memtable.max.bytes`, `triggerMemTableFlush()` is initiated:
   - **Atomic Pre-Rotation**: The active WAL is synced and rotated to a new segment *before* swapping out the active `MemTable`. If WAL rotation fails, the engine transitions to `FAILED` state, rejects writes, and preserves the active MemTable and its WAL segment intact without acknowledged data loss.
   - Active MemTable is sealed and queued for flush with its associated WAL file reference.
   - A `CompletableFuture<File>` tracks flush execution to prevent duplicate concurrent flushes.
   - Post-publication WAL retirement: The associated WAL segment is deleted/retired only *after* the resulting SSTable is fully installed and published.
   - `retryFailedFlushes()` allows retrying any failed flushes to recover from transient disk or I/O errors.

---

## 3. Read Path & Version-Aware Consistency

Reads (`get(byte[] key)`) execute version-aware candidate selection under `rwLock.readLock()`:

1. **Active MemTable Lookup**: The active in-memory skip list is checked.
2. **Immutable MemTables Lookup**: All sealed MemTables pending or undergoing background flush are scanned.
3. **SSTable Lookups**:
   - Boundary check: If the target key falls outside `[minKey, maxKey]`, the SSTable is skipped.
   - Bloom Filter check: Kirsch-Mitzenmacher MurmurHash3 filter skips tables with zero disk I/O.
   - Sparse Index binary search: Identifies the specific `DataBlock`.
   - Single-block disk read: The data block is read, CRC32 verified, and evaluated.
4. **Candidate Resolution**: The candidate record with the **strictly highest sequence number** across all layers is chosen.
   - If the winning record is a tombstone (`RecordType.DELETE`), `null` is returned.
   - If the winning record is a PUT, the record is returned.

---

## 4. SSTable Iterator Resource Management

`SSTableReader.iterator()` returns a `CloseableIterator<DataRecord>`:
- Acquires an SSTable reference count upon creation.
- Automatically releases the reference count when the iterator is fully consumed.
- Releases the reference count immediately upon explicit `close()` or if an exception occurs during block loading.
- `CompactionEngine` processes all source iterators in try-with-resources blocks, ensuring references are never leaked during successful or failed compaction passes.
