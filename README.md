<div align="center">

# ⚡ AetherDB
### High-Performance Hybrid Key-Value & Columnar Storage Engine Prototype
*Engineered in pure modern **Java (21–26)** with Virtual Threads, Zero External Dependencies, and Vectorized Aggregation.*

<br/>

[![Java CI](https://github.com/yashwantraj147-eng/AetherDB/actions/workflows/ci.yml/badge.svg)](https://github.com/yashwantraj147-eng/AetherDB/actions/workflows/ci.yml)
[![Java 21+](https://img.shields.io/badge/Java-21%2B%20%2F%2026-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Architecture](https://img.shields.io/badge/Architecture-LSM%20%2B%20Columnar-38BDF8?style=for-the-badge)](docs/architecture.md)
[![Consensus](https://img.shields.io/badge/Consensus-Raft%20(Prototype)-10B981?style=for-the-badge)](docs/architecture.md)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg?style=for-the-badge)](LICENSE)

<br/>

<a href="#benchmarks">📊 Performance Benchmarks</a> &bull;
<a href="#architecture">🏛️ Architecture</a> &bull;
<a href="#quickstart">🚀 Quickstart</a> &bull;
<a href="#web-studio">✨ Web Studio UI</a> &bull;
<a href="#api-reference">📡 API Reference</a> &bull;
<a href="docs/architecture.md">📖 Deep Internals Docs</a>

</div>

---

## 🌟 Overview

**AetherDB** is an educational, high-performance prototype storage engine exploring the convergence between operational key-value stores and analytical column engines:

1. **Transactional LSM Tier**: Sequential append-only Write-Ahead Logging (WAL) with CRC32 frame checksums, memory skip-list tables (`ConcurrentSkipListMap`), sparse-indexed SSTables, and Kirsch-Mitzenmacher Bloom filtering.
2. **Analytical Columnar Tier**: Adaptive Run-Length (RLE) and Dictionary encoding with block Min-Max zone-map statistics for vectorized predicate pushdown.
3. **Consensus & Clustering (Prototype)**: In-memory Raft state machine simulation featuring term election timers and consistent hashing with 64 virtual nodes.

Built in pure Java with **zero third-party runtime dependencies**, utilizing Java virtual threads (`Thread.ofVirtual()`) and custom binary serialization.

---

## 🏛️ System Architecture

Detailed technical documentation and crash recovery protocols can be reviewed in [docs/architecture.md](docs/architecture.md).

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
  │  • Multi-Level SSTables (.sst / .idx)    │                    │  • Loop-Unrolled Predicate Pushdown     │
  │  • Kirsch-Mitzenmacher Bloom Filters     │                    │  • Data Skipping Predicate Evaluation   │
  │  • Multi-Way Merge-Sort Compaction       │                    └────────────────────┬────────────────────┘
  └────────────────────┬─────────────────────┘                                         │
                       │                                                               │
                       └───────────────────────────────┬───────────────────────────────┘
                                                       │
                                                       ▼
                                      ┌──────────────────────────────────┐
                                      │ Raft Consensus (Experimental)   │
                                      │ • Leader Election (Heartbeats)   │
                                      │ • Replicated AppendEntries Log   │
                                      │ • Consistent Hashing (64 vnodes) │
                                      └──────────────────────────────────┘
```

---

## 📊 Benchmark Methodology & Results

Microbenchmarks captured using `com.aetherdb.cli.AetherBenchmark` on an x86_64 host (OpenJDK 26, G1GC, 10,000 operation warm-up phase):

| Workload | Concurrency | Total Operations | Latency / Time | Measured Throughput | Operational Notes |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrent Writes (`PUT`)** | 4 Virtual Threads | 80,000 Ops | 1,657 ms | **~48,200 IOPS** | Append to WAL + MemTable insertion |
| **In-Memory Point Lookups (`GET`)** | 4 Virtual Threads | 80,000 Ops | 112 ms | **~714,000 IOPS** | Warm lookups resolved from active MemTable |
| **Vectorized Column Scan** | Batch size: 1024 | 1,000,000 Rows | 1.84 ms | **High-throughput** | Zone-Map block skipping evaluated |

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
- **Distributed Consensus & Partitioning (Experimental Prototype)**:
  - Full **Raft Consensus Protocol** implementation with randomized election timers, heartbeat broadcasting, term verification, and replicated log state.
  - **Consistent Hash Ring** allocating 64 Virtual Nodes (`vnodes`) per physical node using MurmurHash3-32 for uniform key distribution.
- **Zero-Dependency Architecture**:
  - Pure modern Java (21–26 compatible).
  - Embedded HTTP 1.1 server leveraging Java Virtual Threads for non-blocking I/O.

---

## 🚀 Quickstart

### Prerequisites
- **Java SE 21+** or **Java SE 26** installed (`java -version`).
- **Maven 3.8+** (optional, standard build script provided).

### 1. Build AetherDB
- **Maven:**
  ```bash
  mvn clean compile
  ```
- **Windows (PowerShell / CMD without Maven):**
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
# or via Maven:
mvn test
```

### 3. Launch the Server
```powershell
.\run.ps1
```
*(Or run `run.bat` on Windows / `./run.sh` on Linux)*

---

## 🖥️ AetherDB Studio (Web Cockpit)

Once the engine starts, navigate to:
```
http://127.0.0.1:8080/dashboard
```

The embedded Studio features:
1. **Telemetry & Live Sparkline**: Real-time IOPS graph updated on a live 2000ms ticker, MemTable memory bytes, and Bloom filter miss savings.
2. **LSM Key-Value Explorer**: Interactive CRUD operations (`PUT`, `GET`, `DELETE`) with formatted JSON terminal views.
3. **Vectorized Analytics Studio**: One-click synthetic IoT telemetry generator, preset analytical queries (`COUNT(*)`, `AVG(latency)`, `MAX(latency)`), and execution timing badges.
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

[MIT](LICENSE) © 2026 Yashwant Raj
