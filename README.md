<div align="center">

# ⚡ AetherDB V2
### Dependable Hybrid LSM Key-Value & Columnar Analytics Storage Engine
*Engineered in modern **Java 21+** with Virtual Threads, Durable Multi-Segment WAL, Crash Recovery, and Vectorized Aggregations.*

<br/>

[![Java CI](https://github.com/yashwantraj147-eng/AetherDB/actions/workflows/ci.yml/badge.svg)](https://github.com/yashwantraj147-eng/AetherDB/actions/workflows/ci.yml)
[![Java 21+](https://img.shields.io/badge/Java-21%2B-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Maven 3.9+](https://img.shields.io/badge/Build-Maven%203.9-C71A36?style=for-the-badge&logo=apachemaven&logoColor=white)](https://maven.apache.org/)
[![Architecture](https://img.shields.io/badge/Architecture-LSM%20%2B%20Columnar-38BDF8?style=for-the-badge)](docs/architecture.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

<br/>

<a href="#benchmarks">📊 Performance Benchmarks</a> &bull;
<a href="#architecture">🏛️ Architecture</a> &bull;
<a href="#quickstart">🚀 Quickstart</a> &bull;
<a href="#api-reference">📡 API Reference</a> &bull;
<a href="docs/architecture.md">📖 Deep Internals Docs</a> &bull;
<a href="CHANGELOG.md">📝 Changelog</a>

</div>

---

## 🌟 Overview

**AetherDB V2** is a dependable, well-engineered database systems prototype featuring correct storage semantics, crash recovery, version-aware reads, and real analytical execution:

1. **Transactional LSM Tier**: Sequential append-only Write-Ahead Logging (WAL) with monotonic segment IDs, bounded torn-tail crash recovery, CRC32 frame checksums, version-enforcing skip-lists, sparse-indexed SSTables, and streaming k-way merge compaction.
2. **Analytical Columnar Tier**: Adaptive Run-Length (RLE) and Dictionary encoding with block Min-Max Zone Maps, independent aggregate state accumulators, and GROUP BY execution.
3. **HTTP REST Gateway & Security**: Java Virtual Threads (`Thread.ofVirtual()`), Jackson Databind JSON parsing, strict HTTP method enforcement (405), request body bounds, and configurable Bearer token authentication.
4. **Consensus & Partitioning**: Collision-safe virtual node consistent hash router and experimental Raft simulation prototype ([Design Spec](docs/raft-design.md)).

---

## 🏛️ System Architecture

Detailed engineering specifications are available in the [docs/](docs/) directory:
- [Storage Engine Internals](docs/storage-engine.md)
- [WAL & Crash Recovery Mechanics](docs/wal-recovery.md)
- [SSTable Compaction & Lifecycle](docs/compaction.md)
- [Columnar Storage & Ingestion](docs/columnar-storage.md)
- [Query Engine & Predicate Pushdown](docs/query-engine.md)
- [REST API & Security Controls](docs/rest-api.md)
- [Raft Status & Prototype Scope](docs/raft-status.md)
- [Production Raft Design Specification](docs/raft-design.md)
- [Configuration Reference](docs/configuration.md)
- [Testing Architecture](docs/testing.md)

---

## 📊 Grounded Benchmarks

Measured on a standard x86_64 host (Windows 11, OpenJDK 26 compiling Java 21 release, G1GC, 1,000 op warm-up, `System.nanoTime()` sampling via `com.aetherdb.cli.AetherBenchmark`):

### Key-Value Operations (20,000 Operations across 4 Virtual Threads)
| Workload | Throughput | Latency p50 | Latency p95 | Latency p99 | Failure Rate |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrent PUT** (WAL + MemTable) | **29,510 IOPS** | 0.022 ms | 0.600 ms | 1.764 ms | 0.00% (0 / 20,000) |
| **Concurrent GET** (Active MemTable) | **323,139 IOPS** | 0.007 ms | 0.021 ms | 0.053 ms | 0.00% (Hit Ratio: 100%) |
| **Concurrent DELETE** (WAL Tombstone) | **62,104 IOPS** | 0.014 ms | 0.275 ms | 0.646 ms | 0.00% (0 / 20,000) |

### Columnar Analytics (100,000 Rows x 20 Iterations)
- **Query**: `SELECT COUNT(*), SUM(amount), AVG(amount) WHERE amount >= 50.0`
- **Execution Time**: **8.05 ms** average latency per query.
- **Scan & Aggregate Throughput**: **12,414,919 rows/sec**.

---

## 🚀 Quickstart

### Prerequisites
- **Java SE 21+** installed (`java -version`).
- **Apache Maven 3.8+** installed (`mvn -version`).

### 1. Build AetherDB
```bash
# Canonical Maven Build
mvn clean package -DskipTests

# Or using OS build scripts:
build.bat    # Windows
./build.sh   # Linux / macOS
```

### 2. Run Verification & Regression Test Suite
Runs all 25 unit and integration tests across WAL recovery, torn tail, CRC checks, SSTable integrity, versioned GET, atomic PUT/DELETE concurrency, streaming compaction, independent aggregates, boundary queries, schema validation, and REST security:
```bash
mvn clean test
```

### 3. Launch AetherDB Server
```bash
# Windows
run.bat
# Linux / macOS
./run.sh
```

---

## 📡 REST API Reference

### Key-Value Store

#### Insert / Update Key (`POST` only)
```bash
curl -X POST http://127.0.0.1:8080/api/v1/kv/put \
  -H "Content-Type: application/json" \
  -d '{"key":"user:101","value":"active-session"}'
```

#### Read Key (`GET` only)
```bash
curl "http://127.0.0.1:8080/api/v1/kv/get?key=user:101"
```

#### Delete Key (`DELETE` or `POST`)
```bash
curl -X DELETE "http://127.0.0.1:8080/api/v1/kv/delete?key=user:101"
```

---

### Columnar Analytics

#### Batch Ingest Records (`POST` only)
```bash
curl -X POST http://127.0.0.1:8080/api/v1/analytics/ingest \
  -H "Content-Type: application/json" \
  -d '{
    "table": "events",
    "records": [
      {"id": 1, "region": "us-east", "latency": 14.5, "status": 200},
      {"id": 2, "region": "us-west", "latency": 22.1, "status": 200}
    ]
  }'
```

#### Run Analytical Query with Aggregates & Grouping (`POST` only)
```bash
curl -X POST http://127.0.0.1:8080/api/v1/analytics/query \
  -H "Content-Type: application/json" \
  -d '{
    "table": "events",
    "select": ["COUNT(*)", "AVG(latency)"],
    "groupBy": ["region"],
    "limit": 50
  }'
```

---

## 📜 License

[MIT](LICENSE) © 2026 Yashwant Raj
