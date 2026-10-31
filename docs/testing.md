# Verification & Automated Regression Test Suite

## 1. Test Architecture

AetherDB V2 uses JUnit 5 (`org.junit.jupiter`) as the canonical automated test framework executed via the Maven Surefire plugin.

All tests operate against isolated temporary directories created with `Files.createTempDirectory()` and thoroughly tear down disk resources on completion, ensuring test repeatability across clean environments.

---

## 2. Test Suite Overview (25 Tests Across 8 Categories)

| Test Class | Focus & Scenarios Covered |
| :--- | :--- |
| **`WalRecoveryTest`** | Multi-segment chronological replay; monotonic segment numbering; safe torn-tail truncation on partial writes at EOF; middle-of-file CRC corruption rejection (`CorruptedWalException`); invalid magic bytes rejection. |
| **`LsmConcurrencyAndVersionTest`** | Version-aware GET across active MemTable, flushing MemTables, and multi-version SSTables; tombstone masking of older records; sequence number persistence across restart; multi-threaded concurrent PUT and DELETE determinism. |
| **`SSTableIntegrityTest`** | SSTable write/read round trip; DataBlock CRC32 verification and bit-flip tampering detection; crash safety when incomplete `.tmp` temporary files exist in the SSTable directory. |
| **`CompactionStreamingTest`** | Streaming multi-way merge sort de-duplication preserving the highest sequence number; tombstone pruning logic; memory-bounded streaming iterator. |
| **`QueryEngineCorrectnessTest`** | Independent aggregate state isolation across multiple expressions (`COUNT(*)`, `COUNT(col)`, `SUM`, `AVG`, `MIN`, `MAX`); non-null counting in `COUNT(col)` and `AVG(col)`; boundary comparison operators (`>=`, `<=`, `!=`, `LIKE`); ZoneMap boundary pruning. |
| **`ColumnarStorageTest`** | Long string encoding (5,000+ characters), empty strings, and Unicode characters without buffer overflow; path traversal security rejection on table names; schema mismatch validation; chunk row partitioning. |
| **`RestApiSecurityTest`** | HTTP method enforcement (405 on invalid method); Jackson JSON serialization and parsing; real batch analytics ingestion; configurable query execution with grouping and filters; payload limits; authentication. |
| **`ConsistentHashRouterTest`** | Partition ring routing; collision safety preventing virtual nodes from overwriting each other on hash collision. |

---

## 3. Running the Test Suite

```bash
# Run authoritative JUnit test suite via Maven
mvn clean test

# Or run Windows / Linux verification scripts:
test.bat     # Windows
./test.sh    # Linux / macOS
```
