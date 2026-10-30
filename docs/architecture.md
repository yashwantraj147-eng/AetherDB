# AetherDB Architecture & Storage Engine Internals

This document details the architectural layout, durability guarantees, compaction algorithms, and distributed state machine mechanics of **AetherDB**.

---

## 1. LSM-Tree Storage Tier (Key-Value Engine)

AetherDB utilizes a Log-Structured Merge-Tree (LSM) architecture optimized for high-write velocity with bounded read amplification.

```
Write Request (PUT k, v)
        │
        ├──► 1. Append to Write-Ahead Log (WAL) [Disk / fsync]
        │
        └──► 2. Insert into MemTable [RAM: ConcurrentSkipListMap]
                    │
                    ▼ (When MemTable capacity reached)
             3. Flush to Disk as Immutable SSTable (Level 0)
                    │
                    ▼
             4. Background Multi-Way Merge-Sort Compaction
```

### 1.1 Write-Ahead Log (WAL) & Crash Recovery
- **Framing Format**: Every log entry is prefixed with a 4-byte CRC32 checksum, followed by a 1-byte operation tag (`PUT` or `DELETE`), 4-byte key length, key bytes, 4-byte value length, and value bytes.
- **Recovery Procedure**: Upon engine reboot:
  1. The WAL file is scanned sequentially from byte offset 0.
  2. For every record, CRC32 is recomputed and compared against the recorded frame header. Corrupt trailing fragments caused by abrupt power loss are detected and cleanly truncated.
  3. Valid records are replayed into a fresh MemTable before the server accepts inbound traffic.
- **Durability Modes**:
  - `IMMEDIATE`: Explicit `FileChannel.force(false)` per write.
  - `PERIODIC`: Background flusher commits dirty OS pages at configurable millisecond intervals.
  - `OS_BUFFER`: Relies on OS virtual memory page flushing for maximum throughput.

### 1.2 SSTable Structure & Bloom Filters
- **Data Block (`.sst`)**: Stores sorted key-value pairs with variable-byte length prefixes.
- **Index Block (`.idx`)**: Sparse binary index recording every $N$-th key alongside its byte offset in the corresponding `.sst` file.
- **Bloom Filters**: Uses double-hashing (Kirsch-Mitzenmacher technique) based on MurmurHash3 to guarantee $O(1)$ negative lookups. If the filter returns false, disk I/O is skipped completely.

### 1.3 Multi-Way Merge-Sort Compaction
- Compaction merges multiple overlapping Level 0 SSTables into sorted Level 1+ tables.
- Employs a multi-way min-heap iterator.
- Eliminates tombstones (soft-deleted records) whose TTL or transaction visibility boundary has expired.

---

## 2. Vectorized Columnar Analytics Tier

For analytical workloads requiring scans over millions of records across subset projections, AetherDB provides a columnar encoding layer.

### 2.1 Adaptive Compression Encoders
- **Run-Length Encoding (RLE)**: Encodes consecutive repeated values as `(count, value)` pairs. Ideal for status flags, dates, and sorted dimensions.
- **Dictionary Encoding**: Assigns unique integer IDs to distinct values, storing an index dictionary followed by a bit-packed ID stream. Ideal for categorical fields (e.g., country codes, HTTP methods).

### 2.2 Zone-Map Predicate Pushdown
- Every data block maintains block-level metadata:
  $$\text{min\_val}, \quad \text{max\_val}, \quad \text{null\_count}$$
- When executing filtered queries (e.g. `WHERE latency > 500`), the query planner compares the filter predicate against block Min-Max boundaries. Blocks that cannot satisfy the predicate are skipped entirely without decompression.

---

## 3. Distributed Consensus & Clustering (Experimental)

> [!NOTE]
> The distributed Raft consensus and multi-node cluster coordinator are prototype implementations designed for architectural research and local multi-node simulation.

- **Raft State Machine**:
  - Randomized election timeouts (150ms–300ms) to avoid split votes.
  - Heartbeat broadcasting and term validation via `AppendEntries` RPC.
  - Linearizable state transitions logged before local apply.
- **Partitioning & Virtual Nodes**:
  - Consistent hash ring with 64 virtual nodes (`vnodes`) per physical instance using MurmurHash3-32.
  - Provides balanced key distribution and minimal reassignment churn on node joins/failures.

---

## 4. Benchmark Methodology & Hardware Context

Microbenchmarks reported in the project were captured under the following test harness:
- **Processor**: AMD Ryzen / Intel x86_64, 4-8 cores allocated.
- **Runtime**: OpenJDK 21 / 26, `-Xms2G -Xmx4G -XX:+UseG1GC`.
- **Harness**: Sequential warm-up runs of 10,000 operations prior to measured 80,000 operation sampling.
- **Hit Ratio Definition**: 100% cache hit ratio indicates queries answered from active memory tier (MemTable + cached SSTable sparse index block) without cold disk seek delays.
