# AetherDB V2 — Engineering Changelog

All notable changes and architectural repairs in **AetherDB V2** are documented in this file.

---

## [2.0.0] - 2026-10-31

### Storage Engine & WAL Reliability (Priority 0)
- **Monotonic WAL Segments**: Implemented monotonically identified WAL segment files (`wal_%08d.wal`), replacing millisecond timestamp file collision risks.
- **Multi-Segment Chronological Recovery**: Restored full recovery across all valid `.wal` segments in chronological sequence instead of only `active.wal`.
- **Torn-Tail Crash Recovery & Safe Truncation**: Bounded frame-by-frame parsing detecting incomplete or interrupted trailing writes at EOF, truncating the file channel safely to `lastValidOffset` and allowing clean subsequent writes.
- **Checksum Verification**: Enforced CRC32 verification on deserialization of all WAL frames and SSTable data blocks (`CorruptedWalException`, `CorruptedSSTableException`).
- **Sequence Number Persistence**: Initialized sequence generator from the maximum sequence number across all recovered WAL records and all existing SSTable metadata, guaranteeing sequence monotonicity across crash and restart.
- **Atomic Write Ordering**: Serialized sequence generation, WAL append, and MemTable insertion under atomic synchronization, preventing lower-sequence writes from racing ahead of higher-sequence writes.
- **Version-Aware Storage Reads**: Implemented global sequence-aware resolution across active MemTable, flushing immutable MemTables, and SSTables. Newer tombstones properly hide older persisted values.
- **Crash-Safe File Installation**: Implemented temporary file installation (`.tmp`) with atomic rename (`StandardCopyOption.ATOMIC_MOVE`) for SSTables and Columnar Chunks.

### Concurrency & Compaction (Priority 1)
- **Compaction Publication Isolation**: Modified compaction publication under `rwLock.writeLock()` to remove only participating snapshot SSTables, ensuring concurrent flushes installed during compaction are never clobbered.
- **Streaming Multi-Way Merge**: Replaced full in-memory list materialization during compaction with a streaming `PriorityQueue` iterator, reducing compaction heap usage from $O(N)$ to $O(K)$ tables.
- **Reader-Safe SSTable Retirement**: Added atomic reference counting to `SSTableReader` (`acquireRef` / `releaseRef`), preventing readers from accessing closed file channels or deleted files.
- **Executor Bounded Backpressure**: Replaced unbounded background flush queues with a bounded thread pool and `CallerRunsPolicy` to prevent out-of-memory under heavy write bursts.

### Query Engine & Columnar Storage (Priority 2)
- **Independent Aggregate Accumulators**: Replaced shared aggregate state in `QueryEngine` with isolated accumulators per expression (`CountStarAccumulator`, `CountColumnAccumulator`, `SumAccumulator`, `AvgAccumulator`, `MinAccumulator`, `MaxAccumulator`).
- **Null Semantics**: Fixed `COUNT(column)` to exclude null values and `AVG(column)` to divide strictly by non-null counts.
- **Inclusive Boundary Operators**: Fixed `>=` and `<=` predicate evaluations and ZoneMap skipping rules to include boundary values.
- **Operator Support**: Implemented `NOT_EQUALS` and SQL `LIKE` pattern matching with `%` and `_` wildcards.
- **Real GROUP BY Support**: Implemented multi-group aggregation with dedicated accumulator buckets per group key.
- **Dynamic Encoder Buffer Sizing**: Replaced fixed 16-byte buffer estimates in `ColumnEncoder` with exact capacity calculation, supporting long strings (5,000+ chars), empty strings, and Unicode without buffer overflow.
- **Path Traversal Security**: Sanitized table names against directory traversal sequences (`../`, `/`, `\`).

### REST API & Security (Priority 3)
- **Strict HTTP Method Enforcement**: Added 405 Method Not Allowed validation for all routes.
- **Jackson Core Serialization**: Replaced fragile hand-rolled JSON string splitting with Jackson Databind (`ObjectMapper`).
- **Real Batch Analytics Ingestion**: Implemented multi-row batch ingestion parsing real JSON payloads.
- **Configurable Analytics Querying**: Enabled arbitrary JSON queries with column selections, predicates, operators, and grouping.
- **Security Hardening**: Added request body size limits (10 MB default), configurable Bearer token authentication, and configurable CORS origins.

### Distributed Scope & Build System (Priority 4 & 5)
- **Collision-Safe Consistent Hashing**: Implemented `TreeMap<Integer, List<VirtualNode>>` ring to prevent virtual node overwrite on 32-bit MurmurHash3 collisions.
- **Honest Raft Status**: Documented Raft consensus as an experimental simulation prototype, documented KV local write path, and created comprehensive production design specification (`docs/raft-design.md`).
- **Defensive Immutability**: Implemented defensive cloning of key and value byte arrays in `DataRecord`.
- **Unified Java 21 Build System**: Aligned Maven compiler, Windows scripts (`build.bat`, `build.ps1`, `test.bat`, `run.bat`), Unix scripts (`build.sh`, `test.sh`, `run.sh`), and GitHub Actions CI workflow to Java 21 canonical build.
- **Comprehensive Automated Regression Tests**: Created 25 automated JUnit 5 unit and integration tests across 8 categories with 100% pass rate.

