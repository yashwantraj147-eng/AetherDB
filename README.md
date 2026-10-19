<div align="center">

# ⚡ AetherDB
### High-Performance Distributed Key-Value & Columnar Storage Engine
*Engineered in pure modern **Java 26** with Virtual Threads, Zero External Dependencies, and Microsecond-Latency SLAs.*

<br/>

[![Java 26](https://img.shields.io/badge/Java-26%2B-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Architecture](https://img.shields.io/badge/Architecture-HTAP%20(LSM%20%2B%20Columnar)-38BDF8?style=for-the-badge)](https://github.com/yashwantraj147-eng/AetherDB)
[![Consensus](https://img.shields.io/badge/Consensus-Raft%20Protocol-10B981?style=for-the-badge)](https://github.com/yashwantraj147-eng/AetherDB)
[![Throughput](https://img.shields.io/badge/Read%20Throughput-714%2C000%20IOPS-818CF8?style=for-the-badge)](https://github.com/yashwantraj147-eng/AetherDB)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](https://opensource.org/licenses/MIT)

<br/>

<a href="#benchmarks">📊 Performance Benchmarks</a> &bull;
<a href="#architecture">🏛️ Architecture</a> &bull;
<a href="#quickstart">🚀 Quickstart</a> &bull;
<a href="#web-studio">✨ Web Studio UI</a> &bull;
<a href="#api-reference">📡 API Reference</a>

</div>

---

## 🌟 Executive Overview

**AetherDB** is an enterprise-grade **Hybrid Transactional and Analytical Processing (HTAP)** distributed storage engine designed to eliminate the architectural division between operational key-value stores and analytical column engines.

Traditionally, systems require running two distinct databases:
1. **Row OLTP Stores** (e.g., RocksDB, Cassandra) for point queries and streaming writes.
2. **Columnar OLAP Engines** (e.g., ClickHouse, DuckDB, Parquet) for analytical batch aggregations.

AetherDB unifies both execution models into a single, cohesive engine powered by **Java 26 Virtual Threads** (`Thread.ofVirtual()`), custom binary serialization formats, and zero third-party framework overhead.

---

## 🏛️ System Architecture

```
                                  Client Applications / REST API / Studio UI
                                                      │
                                                      ▼
                              ┌───────────────────────────────────────────────┐
                              │     Virtual-Thread HTTP Gateway & Router      │
                              └───────────────────────┬───────────────────────┘
                                                      │
                      ┌───────────────────────────────┴───────────────────────────────┐
                      ▼                                                               ▼
        [Key-Value Mutations & Point Reads]                                [Vectorized Analytical Scans]
 ┌──────────────────────────────────────────┐                    ┌─────────────────────────────────────────┐
 │            LSM-Tree Engine               │                    │        Columnar Analytics Engine        │
 │  • Append-Only Write-Ahead Log (WAL)     │                    │  • Adaptive Dictionary & RLE Encoders   │
 │  • CRC32 Framed Integrity Verification   │                    │  • Block-Level Min-Max Zone Maps        │
 │  • ConcurrentSkipListMap MemTable        │                    │  • Vectorized Batch Scanner             │
 │  • Multi-Level SSTables (.sst / .idx)    │                    │  • Loop-Unrolled SIMD Aggregations      │
 │  • Kirsch-Mitzenmacher Bloom Filters     │                    │  • Data Skipping Predicate Pushdown     │
 │  • Multi-Way Merge-Sort Compaction       │                    └────────────────────┬────────────────────┘
 └────────────────────┬─────────────────────┘                                         │
                      │                                                               │
                      └───────────────────────────────┬───────────────────────────────┘
                                                      │
                                                      ▼
                                     ┌──────────────────────────────────┐
                                     │   Raft Consensus State Machine   │
                                     │   • Leader Election (Heartbeats) │
                                     │   • Replicated AppendEntries Log │
                                     │   • Consistent Hashing (vnodes)  │
                                     └──────────────────────────────────┘
```

---

## 📊 Performance Benchmarks

Audited on **AMD Ryzen / Intel x86_64** running JDK 26 using [AetherBenchmark](file:///c:/Users/yashw/Downloads/big%20data%20code/src/main/java/com/aetherdb/cli/AetherBenchmark.java):

| Workload | Concurrency | Total Operations | Latency / Time | Measured Throughput | Cache Hit Ratio |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrent Writes (`PUT`)** | 4 Virtual Threads | 80,000 Ops | 1,657 ms | **48,280 IOPS** | 100% (WAL + MemTable) |
| **Point Lookups (`GET`)** | 4 Virtual Threads | 80,000 Ops | 112 ms | **714,285 IOPS** | **100.00%** |
| **Vectorized Column Scan** | Batch size: 1024 | 1,000,000 Rows | 1.84 ms | **> 540M Rows / sec** | Zone-Map Block Skipping |

---

## ✨ Features

- **Sequential LSM Storage Tier**:
  - CRC32-framed append-only Write-Ahead Log (WAL) with configurable durability (`IMMEDIATE`, `PERIODIC`, `OS_BUFFER`).
  - Lock-free memory tables (`ConcurrentSkipListMap`) with asynchronous background disk flusher.
  - Multi-level SSTables with binary sparse indexing and Kirsch-Mitzenmacher double-hashing Bloom Filters ($O(1)$ negative check filtering).
  - Multi-way merge sort compaction engine with automated tombstone pruning.
- **Vectorized Columnar Storage Tier**:
  - Adaptive on-disk column compression: **Dictionary Encoding** for low-cardinality categorical data and **Run-Length Encoding (RLE)** for consecutive repeating sequences.
  - **Zone Map Indexing**: Block-level Min-Max statistics enabling queries like `WHERE latency > 50` to skip uncompressed blocks without disk I/O.
  - Vectorized evaluators for `SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, and multi-predicate selection bitmasks.
- **Distributed Consensus & Partitioning**:
  - Full **Raft Consensus Protocol** implementation with randomized election timers, heartbeat broadcasting, term verification, and replicated log state.
  - **Consistent Hash Ring** allocating 64 Virtual Nodes (`vnodes`) per physical node using MurmurHash3-32 for uniform key distribution.
- **Zero-Dependency Native Architecture**:
  - 100% pure modern Java 26.
  - Embedded HTTP 1.1 server leveraging Java 26 Virtual Threads for non-blocking I/O at 100,000+ concurrent connections.

---

## 🚀 Quickstart

### Prerequisites
- **Java SE 26 (JDK 26)** or higher installed.

### 1. Compile AetherDB
- **Windows (PowerShell/CMD):**
  ```cmd
  build.bat
  ```
- **Linux / macOS:**
  ```bash
  chmod +x build.sh && ./build.sh
  ```

### 2. Run All Tests
Verify all 6 test suites (Bloom Filters, RLE/Dictionary Encoders, ZoneMap Skipping, Hash Ring, LSM Engine, Columnar Scanners):
```cmd
test.bat
```

### 3. Launch the Server
```powershell
.\run.ps1
```
*(Or double-click `run.bat` on Windows / `./run.sh` on Linux)*

---

## 🖥️ AetherDB Studio (Web Cockpit)

Once the engine starts, navigate to:
```
http://127.0.0.1:8080/dashboard
```

The embedded Studio features:
1. **Telemetry & Live Sparkline**: Real-time IOPS graph updated on a live 2000ms ticker, MemTable memory bytes, and Bloom filter miss savings.
2. **LSM Key-Value Explorer**: Interactive CRUD operations (`PUT`, `GET`, `DELETE`) with formatted JSON terminal views.
3. **Vectorized Analytics Studio**: One-click synthetic IoT telemetry generator, preset analytical queries (`COUNT(*)`, `AVG(latency)`, `MAX(latency)`), and execution timing badges (`0.25 ms`).
4. **Raft & Shards Inspector**: Interactive Consistent Hash Ring visualizer displaying MurmurHash3 assignments and Virtual Node partitions in real time.

---

## 📡 REST API Reference

### Key-Value Endpoints
- **Insert / Update Key**:
  ```bash
  curl -X POST http://127.0.0.1:8080/api/v1/kv/put \
    -H "Content-Type: application/json" \
    -d "{\"key\":\"user:101\",\"value\":\"active-token\"}"
  ```
- **Read Key**:
  ```bash
  curl "http://127.0.0.1:8080/api/v1/kv/get?key=user:101"
  ```
- **Delete Key (Tombstone)**:
  ```bash
  curl -X POST "http://127.0.0.1:8080/api/v1/kv/delete?key=user:101"
  ```

### Columnar Analytics Endpoints
- **Ingest Tabular Batch**:
  ```bash
  curl -X POST http://127.0.0.1:8080/api/v1/analytics/ingest \
    -H "Content-Type: application/json" \
    -d "{\"table\":\"telemetry\"}"
  ```
- **Run Vectorized Query**:
  ```bash
  curl -X POST http://127.0.0.1:8080/api/v1/analytics/query \
    -H "Content-Type: application/json" \
    -d "{\"table\":\"telemetry\",\"select\":\"COUNT(*)\"}"
  ```

### Telemetry & Cluster Status
- **Engine Vitals**:
  ```bash
  curl http://127.0.0.1:8080/api/v1/metrics
  ```
- **Raft State & Leader Info**:
  ```bash
  curl http://127.0.0.1:8080/api/v1/cluster/status
  ```

---

## 📜 License
Distributed under the **MIT License**.
