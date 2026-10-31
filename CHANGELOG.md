# AetherDB V2 — Engineering Changelog

All notable changes and architectural repairs in **AetherDB V2** are documented in this file.

---

## [2.0.3] - 2026-10-31

### Manual Flush vs Shutdown Race Protection (Priority 0)
- **Active Storage Operation Tracking**: Introduced atomic `activeOperations` counter guarding all public mutating and read/scan operations (`put()`, `delete()`, `get()`, `flushMemTable()`, `compact()`, `registerTableSchema()`).
- **Shutdown Operation Drain Barrier**: Engine shutdown transitions lifecycle state to `CLOSING` to reject new operation admissions, and blocks up to a bounded timeout for all in-flight manual operations and flush tasks to safely complete before closing background executors.
- **Publication-Time Lifecycle Verification**: Both manual and background flush paths verify `lifecycleState == LifecycleState.OPEN` under write lock immediately before installing new SSTable readers. If the engine is `CLOSING` or `CLOSED`, the newly written SSTable is closed and discarded, the MemTable is marked `FAILED`, its future completes exceptionally, and its associated WAL segment is preserved (not retired) for recovery upon restart.
- **Compaction Publication Guard**: Caller-triggered and background compactions verify engine state under write lock before installing replacements; if shutdown intervened, compacted tables are cleanly closed and discarded while input SSTables remain intact.
- **Safe Retryable Shutdown**: If in-flight tasks exceed the bounded shutdown timeout, resources (WAL and SSTable readers) remain open in `CLOSING` state and `close()` throws an `IOException`, permitting safe retry without resource leaks or thread hangs.

### Flush Future and Retry Coordination (Priority 0)
- **Atomic Flush Attempt Claiming**: Implemented `claimFlushAttempt()` on `MemTable` to safely transition `SEALED`, `QUEUED`, or `FAILED` states to `FLUSHING` with a fresh `CompletableFuture<File>` per attempt.
- **Stale Future Reuse Prevention**: Subsequent flush attempts after transient failures never reuse already completed exceptional futures. Concurrent callers safely coordinate and await the active attempt's future.
- **Directory Writeability Verification in Flush Retries**: `retryFailedFlushes()` verifies storage directory writeability with a temporary probe file before clearing `FAILED` state and re-attempting pending flushes.

### Compaction Iterator Constructor Cleanup Safety (Priority 1)
- **Exception-Safe Iterator Initialization**: Wrapped source iterator acquisition and initial entry advancement in `CompactionEngine.StreamingMergeIterator` constructor in a try-catch block.
- **Guaranteed Reader Cleanup on Construction Failure**: If any iterator acquisition or block read fails during iterator construction, all previously opened iterators are safely closed, and any cleanup errors are attached as suppressed exceptions to the primary cause.

### Table Schema Registration & Columnar Ingestion Rules (Priority 1)
- **Strict Table Name Validation**: Enforced pattern validation (`^[a-zA-Z0-9_]{1,128}$`) on table names and verified exact equality between registration argument and `schema.getTableName()`.
- **Idempotent Re-registration**: Allowed re-registering identical schemas idempotently without error.
- **Schema Mutation Protection**: Replaced schema validation ensures incompatible column additions or modified data types are rejected with `IllegalArgumentException`.
- **Loaded Chunk Consistency Check**: Registering a schema for existing tables verifies compatibility across all pre-existing columnar chunks on disk.

### ZoneMap Immutability & Deserialization Hardening (Priority 1)
- **ZoneMap Immutability Flag**: Introduced `freeze()`, `isFrozen()`, and `toImmutable()` on `ZoneMap`. Attempting to modify frozen ZoneMaps via `update()` throws `UnsupportedOperationException`.
- **Chunk Boundary Encapsulation**: `ColumnarChunk` constructor automatically freezes all input ZoneMaps upon chunk construction.
- **Deserialization Sanity Checks**: `ZoneMap.deserialize()` validates record counts (`rowCount >= 0`, `nullCount >= 0`, `rowCount >= nullCount`) and min/max consistency (`min <= max`), rejecting corrupted or malicious stream data with `CorruptedSchemaException`.

### Build & Test Suite Expansion (Priority 2)
- **78 Automated Unit & Regression Tests**: Test suite expanded from 68 to 78 automated JUnit 5 tests covering manual flush vs close races, compaction vs close races, iterator construction cleanup, schema registration constraints, and ZoneMap immutability with 100% pass rate.

---

## [2.0.2] - 2026-10-31

### Storage Engine Lifecycle & Shutdown Safety (Priority 0)
- **Engine Lifecycle State Machine**: Introduced explicit `LifecycleState` (`OPEN`, `CLOSING`, `CLOSED`, `FAILED`). Transition into `CLOSING` immediately rejects any new write operations, flushes, and background compactions.
- **Double-Check State Under Synchronization**: `put()`, `delete()`, and `get()` validate lifecycle state both before and after acquiring engine read/write locks, preventing concurrent close calls from racing ahead and attempting writes against closing or closed storage resources.
- **Graceful Bounded Shutdown with Leak Protection**: Shutdown awaits in-flight background tasks up to a bounded timeout. If tasks do not terminate within the timeout, resources (WAL and active SSTables) are safely left open in `CLOSING` state rather than closed beneath active threads, allowing clean retry.
- **Worker Publication Guard**: Background flush and compaction workers verify engine lifecycle state before installing any SSTables; completed tables produced by tasks cancelled during shutdown are cleanly discarded and closed without polluting the live collection.
- **Thread Safety & Idempotent Close**: Made `close()` concurrent-safe and idempotent using atomic state transitions.

### Flush Recovery & WAL Durability (Priority 0)
- **Pre-Rotated WAL Swapping**: In `triggerMemTableFlush()`, the active WAL segment is rotated *prior* to swapping out the active MemTable. If WAL rotation fails, the engine transitions to `FAILED` state, rejects writes, and preserves the active MemTable and its WAL segment without acknowledged data loss.
- **Asynchronous Flush Ownership**: Tracked MemTable flush lifecycle with `CompletableFuture<File>`, preventing duplicate concurrent flushes and enabling callers to safely await in-flight background flushes.
- **Manual Flush Synchronization**: `flushMemTable()` blocks and awaits any in-flight background flushes before proceeding, guaranteeing all acknowledged memory state is durably installed.
- **Failed MemTable Recovery & Retry**: Added `retryFailedFlushes()` to re-attempt failed MemTable flushes and recover from transient disk or I/O errors.
- **Post-Publication WAL Segment Retirement**: Deferred deletion or obsolescence of WAL segments (`retireAssociatedWal()`) until *after* the resulting SSTable is fully installed and published in the storage engine.

### Binary Deserialization Hardening & Overflow Safety (Priority 1)
- **Defensive Schema Deserialization**: Added explicit allocation bounds to `Schema.deserialize()` (`MAX_COLUMNS = 10,000`, max table name length = 255, max column name length = 255). Rejects negative column counts, duplicate column names, unsupported column type IDs, and truncated buffers with descriptive `CorruptedSchemaException`.
- **SSTable Offset Overflow Protection**: Replaced raw integer/long arithmetic in `SSTableReader` metadata offset calculations with `Math.addExact()`. Verifies that index and Bloom filter regions do not overlap the header, do not overlap each other, and do not exceed the actual file size. Rejects malformed offsets with `CorruptedSSTableException`.

### SSTable Iterator Resource Lifecycle (Priority 1)
- **CloseableIterator Contract**: Introduced `CloseableIterator<DataRecord>` extending `Iterator<DataRecord>` and `AutoCloseable`.
- **Guaranteed Reader Reference Release**: The iterator acquires an SSTable reference count and guarantees its release exactly once upon full consumption, early explicit `close()`, or I/O failure during block loading. Repeated `close()` calls are idempotent and safe.
- **Compaction Source Cleanup**: Wrapped all source iterators in `CompactionEngine.StreamingMergeIterator` with try-with-resources and explicit `close()` in `finally` blocks, eliminating reader reference leaks during successful or failed compaction passes.

### Columnar Schema Inference & Immutability (Priority 2)
- **Strict Untyped Null Ingestion Policy**: Initial columnar ingestion batches containing untyped null values for all rows in a column are strictly rejected with descriptive `IllegalArgumentException`, rather than silently defaulting to `STRING`.
- **Explicit Schema Registration**: Added `registerTableSchema(Schema)` and `registerTableSchema(String, Schema)` on `StorageEngine` and `HybridStorageManager`. Tables with pre-registered schemas permit initial all-null batches while retaining their declared numeric or timestamp types.
- **ColumnarChunk Immutability**: Enforced defensive copying of byte arrays in `ColumnarChunk` constructor and accessors (`getEncodedColumn()`, `getEncodedColumns()`). Decoded cached column lists returned by `getDecodedColumn()` are wrapped in unmodifiable lists.

### REST API Security & CORS Hardening (Priority 2)
- **Vary: Origin Header Reflection**: Added `Vary: Origin` response header whenever dynamically reflecting an allowlisted origin, preventing cache poisoning across disparate web clients.
- **Comprehensive Regression Test Matrix**: Expanded `RestApiSecurityTest` to verify all 10 security and CORS scenarios: missing tokens, invalid tokens, valid Bearer tokens, external binding without auth (rejected), external binding with auth (permitted), allowed origins, rejected origins (403 on preflight, no header reflection on GET), OPTIONS preflight methods and headers, and multi-origin allowlists.

### Build & Test Suite Expansion
- **68 Automated Unit & Regression Tests**: Test suite expanded from 44 to 68 automated JUnit 5 tests covering all lifecycle transitions, shutdown races, WAL pre-rotation safety, closeable iterators, binary bounds, columnar immutability, and CORS scenarios with 100% pass rate.

---

## [2.0.1] - 2026-10-31

### Storage Engine & Crash Safety (Priority 0)
- **Compaction Publication Race Resolution**: Redesigned SSTable replacement as an atomic publication transaction under `rwLock.writeLock()`. Compacted tables are built and validated in complete isolation. Input SSTable readers are never prematurely marked obsolete or closed; retirement is deferred until after the new SSTable is safely installed and active reader references are released. Failed compaction leaves all input SSTables intact without state corruption.
- **Tombstone Resurrection Prevention**: Hardened tombstone-pruning logic across the entire storage lifecycle. Pruning is permitted only when all installed SSTables participate in compaction *and* no immutable MemTables are queued or actively flushing (`hasPendingFlushes() == false`), preventing older in-flight PUT operations from resurrecting deleted records.
- **WAL Corruption Classification & Safe Recovery**: Explicitly separated Case A (incomplete trailing frame header or payload at EOF, safely truncated to `lastValidOffset`) from Case B (complete frame payload with bad CRC32 checksum, throwing `CorruptedWalException` to halt recovery and prevent silent corruption).
- **Streaming WAL Recovery**: Added streaming record consumer `recoverAllSegments(walDir, consumer)` and `recoverFile(file, ch, lock, consumer)` to process records sequentially without allocating all log entries in heap memory.
- **Atomic WAL Rotation**: Pre-allocates and initializes the new segment file before updating `activeWalFile` or closing the prior log, preventing unrecoverable states if rotation fails.

### Columnar Integrity & Schema Consistency (Priority 1)
- **Strict Columnar Checksum & Footer Validation**: Added strict 8-byte CRC32 footer verification in `ColumnarChunk`. Rejects truncated footers (< 8 bytes), missing footers, or trailing garbage bytes (`buffer.remaining() != 8`). Enforced chunk file size limits and row count bounds.
- **Table-Wide Schema Consistency**: Persisted stable schemas per table across ingestion batches. Introduced `ColumnValue.ofNull()` for untyped JSON nulls, preventing premature STRING inference. Ingestion validates all records against existing schema, rejecting unexpected columns and type mismatches while populating missing columns with typed nulls.
- **Idempotent MemTable Flush Lifecycle**: Added explicit `MemTable.State` machine (`ACTIVE`, `SEALED`, `QUEUED`, `FLUSHING`, `INSTALLED`, `FAILED`) with atomic CAS state transitions. Prevents duplicate concurrent flush execution from background tasks or manual REST API triggers.

### Query Engine Semantics & REST Security (Priority 2)
- **64-bit Integer Precision in Aggregations**: Implemented `aggregateSumLong()` in `VectorizedScanner` and integer accumulator branching in `QueryEngine` for `INT32`, `INT64`, and `TIMESTAMP` columns. Preserves exact 64-bit precision without double-precision conversion or IEEE 754 rounding, falling back to double only on arithmetic overflow.
- **SQL-Standard Empty Table & Filter Aggregates**: Queries over non-existent tables or zero-matching filter rows return SQL-compliant results (`COUNT(*) = 0`, `COUNT(col) = 0`, `SUM = null`, `AVG = null`, `MIN = null`, `MAX = null`).
- **Strict GROUP BY & Query Validation**: Enforced strict SQL validation rejecting non-aggregate expressions not present in GROUP BY clauses. Validates all referenced columns and filter predicates against table schemas.
- **REST API Authentication on External Interfaces**: Mandatory Bearer token authentication is enforced when binding to `0.0.0.0` or any non-loopback network interface, preventing unauthorized remote access. Rejects blank or empty tokens.
- **Timing Attack Mitigation**: Implemented constant-time token comparison using `MessageDigest.isEqual()`.
- **Raft Endpoint Isolation**: Returns HTTP 404 for distributed consensus routes (`/raft/*`) when `raft.enabled=false`.
- **HTTP Payload & Syntax Hardening**: Returns HTTP 413 Payload Too Large when request body exceeds configured limit; returns HTTP 400 Bad Request with descriptive error details for malformed JSON payloads.
- **CORS Origin Validation**: Validates incoming `Origin` headers against configured allowlist.
- **SSTable File Integrity Validation**: Verifies 6-byte header magic (`0x41455448`) and version at offset 0; validates footer offsets to guarantee non-overlapping bounds (`6 <= index <= bloom <= footer <= fileSize`); closes file channels deterministically on exception.

### Build Scripts, CI & Benchmarking (Priority 3 & 4)
- **Windows Batch Script Error Propagation**: Replaced parenthesized `%ERRORLEVEL%` blocks with jump labels and `exit /b %ERRORLEVEL%` in `build.bat`, `test.bat`, and `run.bat`, correctly reporting Maven compilation and test failures.
- **Multi-OS GitHub Actions CI Matrix**: Configured automated CI matrix running on both `ubuntu-latest` and `windows-latest` on Java 21, executing `call test.bat` on Windows.
- **Empirical Benchmark Accuracy**: Hardened `AetherBenchmark.java` to track successful versus failed operations separately, calculate failure-aware IOPS, record latency percentiles only for successful requests, report failure totals, and exit with code 1 upon failure.
- **Lifecycle & Shutdown Safety**: Rejects write operations once `HybridStorageManager` is closed; made `close()` idempotent.
- **Expanded Automated Test Suite**: Expanded test suite to 44 automated JUnit 5 tests covering compaction publication, tombstone resurrection, WAL corruption cases, columnar integrity, schema consistency, query semantics, and REST security with 100% pass rate.

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

