# SSTable Compaction & Lifecycle Management

## 1. Streaming Multi-Way Merge

AetherDB replaces full in-memory record materialization with a **streaming k-way merge sort**:

```
 [SSTable Reader 1 Iterator] ──┐
 [SSTable Reader 2 Iterator] ──┼──► [PriorityQueue (Min-Heap)] ──► [Streaming Iterator] ──► [SSTableWriter]
 [SSTable Reader N Iterator] ──┘                                 (Emits newest version     (Streams to disk)
                                                                 of each key on the fly)
```

- **Memory Bounded**: Records are streamed directly from input iterators to the `SSTableWriter` without holding the combined dataset in heap memory.
- **Heap Comparator**: Orders elements by key lexicographically; duplicate keys are ordered by sequence number descending so that the newest record is polled first.
- **Duplicate Key De-duplication**: The streaming iterator records `lastEmittedKey`. Once the newest version of a key is emitted, all older versions are discarded on the fly.

---

## 2. Safe Tombstone Pruning (Resurrection Prevention)

Tombstones (`RecordType.DELETE`) are only discarded during compaction when both of the following conditions are strictly met:
1. **Full SSTable Participation**: All currently installed SSTables in the storage engine participate in the compaction (`isFullCompaction == true`).
2. **No In-Flight MemTable Flushes**: Neither the active MemTable nor any immutable MemTables waiting or actively flushing in the background have pending records (`hasPendingFlushes() == false`).

If any older uncompacted SSTable or any unflushed immutable MemTable exists, tombstones are preserved. This prevents in-flight PUT operations from older MemTables from surfacing after compaction removes the tombstone intended to shadow them.

---

## 3. Concurrency, Atomic Publication & Reader Isolation

1. **Independent Build & Validation**: Compaction constructs and validates the replacement SSTable in complete isolation into temporary files (`.tmp`).
2. **Atomic Publication Transaction**: Under `rwLock.writeLock()`, the manager installs the compacted SSTable and swaps out the participating inputs atomically:
   ```java
   sstables.removeAll(participatingTables);
   sstables.add(compactedReader);
   ```
   Readers always see either the complete pre-compaction storage snapshot or the complete post-compaction storage snapshot. Concurrent flushes installed during compaction are never clobbered.
3. **Deferred Reader Retirement**: Input SSTables are never marked obsolete or closed before the replacement is successfully published. If compaction fails at any stage prior to publication, all input SSTables remain intact, fully readable, and valid.
4. **Reference-Counted Retirement**: After publication, retired `SSTableReader` instances are marked obsolete and closed only once all active concurrent readers release their references (`refCount.decrementAndGet() == 0`), preventing closed-channel exceptions.
