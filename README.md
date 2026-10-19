# AetherDB ⚡

> **High-Performance Distributed Key-Value & Columnar Analytics Storage Engine in Modern Java 26**

AetherDB is a unified hybrid transactional/analytical processing (HTAP) distributed storage engine. It provides microsecond-latency point lookups and high-throughput write streams via an append-only LSM-Tree (Log-Structured Merge-tree), combined with an in-memory and on-disk Columnar Block Engine optimized for vectorized analytical queries (OLAP).

---

## Architecture Overview

```
                                +------------------------+
                                |  Client HTTP/REST API  |
                                +------------------------+
                                             |
                                +------------------------+
                                | Virtual Thread Gateway |
                                +------------------------+
                                      |            |
                  [KV Writes / Reads] v            v [Analytical Queries / Ingest]
                   +---------------------+      +-------------------------------+
                   |   LSM-Tree Engine   |      |   Columnar Analytics Engine   |
                   | - Write-Ahead Log   |      | - Vectorized Scanner          |
                   | - MemTable SkipList |      | - Min-Max Zone Maps           |
                   | - SSTables (Sparse) |      | - Dictionary & RLE Encoders   |
                   | - Bloom Filters     |      +-------------------------------+
                   +---------------------+                     |
                              \                               /
                               +--------------+--------------+
                                              |
                               +-----------------------------+
                               |   Raft Distributed State    |
                               |   & Consistent Hash Ring    |
                               +-----------------------------+
```

---

## Key Features

- **LSM-Tree Storage Engine**:
  - Sequential, CRC32-checksummed Write-Ahead Log (WAL) with configurable sync modes (`IMMEDIATE`, `PERIODIC`, `OS_BUFFER`).
  - Lock-free in-memory `ConcurrentSkipListMap` MemTable.
  - Multi-level SSTables with sparse index blocks and binary serialization.
  - Kirsch-Mitzenmacher double-hashing Bloom Filters (MurmurHash3-128) for $O(1)$ negative lookup filtering.
- **Columnar Analytics Engine**:
  - Block-level Min-Max Zone Maps for immediate predicate pushdown and block skipping.
  - Adaptive columnar compression (Dictionary Encoding, Run-Length Encoding).
  - SIMD/loop-unrolled vectorized scan evaluation (`SUM`, `AVG`, `MIN`, `MAX`, `COUNT`, `FILTER`, `GROUP BY`).
- **Distributed Consensus & Routing**:
  - Built-in Raft consensus state machine (Term election, log replication, heartbeats).
  - Consistent Hash Ring with 64 Virtual Nodes (`vnodes`) per physical node.
- **Modern Java 26 Runtime**:
  - Zero external dependencies.
  - Powered by Java 26 Virtual Threads (`Thread.ofVirtual()`) and Memory-Mapped I/O (`FileChannel`).
  - Embedded real-time management dashboard and metrics API.

---

## Building and Running

### Prerequisites
- Java Development Kit (JDK) 26 or higher.

### Compilation
- **Windows (PowerShell/CMD):**
  ```cmd
  build.bat
  ```
- **Linux / macOS:**
  ```bash
  chmod +x build.sh
  ./build.sh
  ```

---

## License
MIT License.
