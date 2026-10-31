# Benchmarking Methodology & Grounded Performance Report

## 1. Benchmarking Environment

All benchmark numbers reported for AetherDB V2 were measured using the reproducible benchmark harness in `com.aetherdb.cli.AetherBenchmark` under the following test environment:
- **Processor**: x86_64, 4 virtual worker threads allocated.
- **Operating System**: Windows 11 (build 10.0, amd64).
- **JVM**: Oracle Java SE 26.0.1, compiling with Maven `--release 21`.
- **Garbage Collector**: G1GC default.
- **Timing Harness**: High-resolution `System.nanoTime()` sampling with dedicated warm-up cycles (1,000 operations) prior to measurement.

---

## 2. Grounded Performance Results

### Key-Value Storage Engine (20,000 Operations across 4 Concurrent Virtual Threads)

| Operation | Throughput (IOPS) | Latency p50 | Latency p95 | Latency p99 | Failure Rate |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Concurrent PUT** (with WAL & MemTable) | **29,509.8 IOPS** | 0.022 ms | 0.600 ms | 1.764 ms | 0.00% (0 / 20,000) |
| **Concurrent GET** (in-memory MemTable) | **323,138.8 IOPS** | 0.007 ms | 0.021 ms | 0.053 ms | 0.00% (Hit Ratio: 100%) |
| **Concurrent DELETE** (with WAL Tombstone) | **62,103.6 IOPS** | 0.014 ms | 0.275 ms | 0.646 ms | 0.00% (0 / 20,000) |

---

### Columnar Analytical Query Engine (100,000 Records x 20 Iterations)

- **Query**: `SELECT COUNT(*), SUM(amount), AVG(amount) FROM bench_analytics WHERE amount >= 50.0`
- **Scanned Dataset**: 100,000 structured columnar rows per iteration (2,000,000 rows evaluated total).
- **Average Query Execution Time**: **8.05 ms** per query.
- **Scan & Aggregation Throughput**: **12,414,919 rows/sec**.

---

## 3. How to Reproduce

Execute the packaged benchmark harness:
```bash
# Clean build and package shaded executable JAR
mvn clean package -DskipTests

# Run benchmark with 4 worker threads, 5,000 ops/worker
java -cp target/aetherdb-2.0.3.jar com.aetherdb.cli.AetherBenchmark 4 5000
```

