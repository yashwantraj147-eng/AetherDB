# AetherDB Storage Engine Architecture

## Overview
AetherDB implements a dual-engine architecture:
1. **LSM-Tree Key-Value Engine**: Optimized for high-throughput write-intensive transactional mutations with write-ahead logging, memory buffering in `MemTable`, immutable disk-backed sorted string tables (`SSTable`), and background multi-way compaction.
2. **Columnar Analytics Engine**: Optimized for projection and aggregation queries with typed column blocks, Run-Length Encoding (RLE), Dictionary compression, and Min-Max Zone Maps.

---

## 1. Write Path & Durability Protocol

Every write operation (`put` or `delete`) executes under an explicit ordering protocol:

1. **Write Lock Acquisition**: The engine acquires `rwLock.writeLock()`.
2. **Atomic Sequence Generation**: A 64-bit monotonically increasing sequence number is assigned (`sequenceNumberGenerator.getAndIncrement()`).
3. **DataRecord Construction**: Key and value byte arrays are defensively copied to guarantee record immutability.
4. **WAL Append**: The framed record is appended to the active WAL segment (`wal.append(record)`).
   - In `IMMEDIATE` sync mode, `FileChannel.force(false)` is invoked synchronously before proceeding.
   - In `PERIODIC` sync mode, dirty pages are flushed on a background timer (default 50ms).
5. **MemTable Update**: The record is inserted into the active `MemTable`. The `ConcurrentSkipListMap` enforces version monotonicity: an older sequence number will never overwrite a newer sequence number for the same key.
6. **Flush Threshold Evaluation**: If active `MemTable` memory footprint exceeds `lsm.memtable.max.bytes`, `triggerMemTableFlush()` is initiated:
   - Active MemTable is sealed and added to `immutableMemTables`.
   - The active WAL segment is synced and rotated to `wal_%08d.wal`.
   - A `MemTableFlushTask` is dispatched to the background worker pool with the rotated WAL file reference.

---

## 2. Read Path & Version-Aware Consistency

Reads (`get(byte[] key)`) do not rely on insertion order or flush completion order. Instead, they execute version-aware candidate selection:

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

## 3. Data Safety & Defensive Immutability
All internal keys and values exposed through public APIs or stored in `DataRecord` are defensively copied upon construction and getter access (`key.clone()`), preventing external callers from mutating keys after insertion into sorted skip-lists, bloom filters, or block caches.
