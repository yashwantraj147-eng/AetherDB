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

## 2. Safe Tombstone Pruning

Tombstones (`RecordType.DELETE`) are only discarded during compaction when `isDeepestLevel == true` (i.e. all existing SSTables in the database participated in the compaction). If any older unconsidered SSTable exists, tombstones are preserved to prevent older deleted keys from resurrecting.

---

## 3. Concurrency & Reader Isolation

1. **Compaction Snapshot**: Compaction captures an immutable snapshot of SSTables to compact: `tablesToCompact = new ArrayList<>(sstables)`.
2. **Safe Writing**: Compacted records are written to a temporary `.tmp` file and atomically renamed to the target SSTable name.
3. **Atomic Publication**: Under `rwLock.writeLock()`, only the participating tables are removed:
   ```java
   sstables.removeAll(tablesToCompact);
   sstables.add(compactedReader);
   ```
   Any newly installed SSTables created by concurrent flushes during the compaction process are **never clobbered or lost**.
4. **Reader Reference Counting**: Each `SSTableReader` maintains an atomic reference count. Obsolete compacted files remain open until all active point readers and iterators complete, preventing `FileChannel` closed exceptions.
