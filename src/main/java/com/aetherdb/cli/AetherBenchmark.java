package com.aetherdb.cli;

import com.aetherdb.analytics.FilterOperator;
import com.aetherdb.analytics.Predicate;
import com.aetherdb.analytics.QueryEngine;
import com.aetherdb.analytics.QueryRequest;
import com.aetherdb.analytics.QueryResult;
import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.engine.HybridStorageManager;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Empirical Benchmark Harness for AetherDB V2.
 * Accurately measures:
 * 1. Concurrent LSM PUT write throughput and latencies (p50, p95, p99).
 * 2. Point lookup GET read throughput and latencies.
 * 3. Point DELETE throughput and latencies.
 * 4. Vectorized Columnar scan & aggregation throughput (rows/sec).
 *
 * Guarantees:
 * - Only successful operations contribute to latency distribution and throughput metrics.
 * - Non-zero exit status if any benchmark phase encounters failures.
 */
public class AetherBenchmark {

    public static void main(String[] args) throws Exception {
        int threadCount = args.length > 0 ? Integer.parseInt(args[0]) : 4;
        int opsPerThread = args.length > 1 ? Integer.parseInt(args[1]) : 5000;
        int totalOps = threadCount * opsPerThread;

        System.out.println("========================================================");
        System.out.println("  AetherDB Empirical Storage & Analytics Benchmark");
        System.out.println("========================================================");
        System.out.printf("  Concurrency: %d Virtual Thread Workers\n", threadCount);
        System.out.printf("  Operations/Worker: %d\n", opsPerThread);
        System.out.printf("  Total Volume: %d operations\n", totalOps);
        System.out.println("========================================================\n");

        File benchDir = new File("./data/bench_" + System.currentTimeMillis());
        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", new File(benchDir, "wal").getPath());
        config.setProperty("storage.sstable.dir", new File(benchDir, "sstable").getPath());
        config.setProperty("storage.columnar.dir", new File(benchDir, "columnar").getPath());
        config.setProperty("storage.data.dir", new File(benchDir, "storage").getPath());
        config.setProperty("lsm.wal.sync.mode", "PERIODIC");
        config.setProperty("lsm.wal.periodic.sync.ms", "20");
        config.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(config);
        boolean anyFailures = false;

        try (ExecutorService executor = Executors.newFixedThreadPool(threadCount,
                Thread.ofVirtual().name("bench-worker-", 1).factory())) {

            // 1. Warm-up Phase
            System.out.println(">> [1/5] Executing Warm-up Phase (1,000 ops)...");
            for (int i = 0; i < 1000; i++) {
                engine.put("warmup:" + i, "val_" + i);
                engine.get("warmup:" + i);
            }
            System.out.println("   Warm-up completed.\n");

            // 2. PUT Benchmark
            System.out.println(">> [2/5] Running Concurrent PUT Benchmark (" + totalOps + " ops)...");
            ConcurrentLinkedQueue<Long> putLatencies = new ConcurrentLinkedQueue<>();
            CountDownLatch putLatch = new CountDownLatch(threadCount);
            AtomicLong putFailures = new AtomicLong(0);
            long putStartNanos = System.nanoTime();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long opStart = System.nanoTime();
                            String key = "key:" + threadId + ":" + i;
                            String val = "payload_data_val_" + i;
                            try {
                                engine.put(key, val);
                                putLatencies.add(System.nanoTime() - opStart);
                            } catch (Exception ex) {
                                putFailures.incrementAndGet();
                            }
                        }
                    } finally {
                        putLatch.countDown();
                    }
                });
            }

            putLatch.await();
            long putTotalNanos = System.nanoTime() - putStartNanos;
            printBenchmarkSummary("PUT", totalOps, putTotalNanos, putFailures.get(), putLatencies);
            if (putFailures.get() > 0) anyFailures = true;

            // 3. GET Benchmark
            System.out.println(">> [3/5] Running Concurrent GET Benchmark (" + totalOps + " ops)...");
            ConcurrentLinkedQueue<Long> getLatencies = new ConcurrentLinkedQueue<>();
            CountDownLatch getLatch = new CountDownLatch(threadCount);
            AtomicLong getHits = new AtomicLong(0);
            AtomicLong getFailures = new AtomicLong(0);
            long getStartNanos = System.nanoTime();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long opStart = System.nanoTime();
                            String key = "key:" + threadId + ":" + i;
                            try {
                                if (engine.get(key) != null) {
                                    getHits.incrementAndGet();
                                }
                                getLatencies.add(System.nanoTime() - opStart);
                            } catch (Exception ex) {
                                getFailures.incrementAndGet();
                            }
                        }
                    } finally {
                        getLatch.countDown();
                    }
                });
            }

            getLatch.await();
            long getTotalNanos = System.nanoTime() - getStartNanos;
            printBenchmarkSummary("GET", totalOps, getTotalNanos, getFailures.get(), getLatencies);
            System.out.printf("   Hit Ratio: %.2f%%\n\n", (getHits.get() * 100.0 / totalOps));
            if (getFailures.get() > 0) anyFailures = true;

            // 4. DELETE Benchmark
            System.out.println(">> [4/5] Running Concurrent DELETE Benchmark (" + totalOps + " ops)...");
            ConcurrentLinkedQueue<Long> delLatencies = new ConcurrentLinkedQueue<>();
            CountDownLatch delLatch = new CountDownLatch(threadCount);
            AtomicLong delFailures = new AtomicLong(0);
            long delStartNanos = System.nanoTime();

            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(() -> {
                    try {
                        for (int i = 0; i < opsPerThread; i++) {
                            long opStart = System.nanoTime();
                            String key = "key:" + threadId + ":" + i;
                            try {
                                engine.delete(key);
                                delLatencies.add(System.nanoTime() - opStart);
                            } catch (Exception ex) {
                                delFailures.incrementAndGet();
                            }
                        }
                    } finally {
                        delLatch.countDown();
                    }
                });
            }

            delLatch.await();
            long delTotalNanos = System.nanoTime() - delStartNanos;
            printBenchmarkSummary("DELETE", totalOps, delTotalNanos, delFailures.get(), delLatencies);
            if (delFailures.get() > 0) anyFailures = true;

            // 5. Columnar Analytical Scan Benchmark
            System.out.println(">> [5/5] Running Columnar Analytics Scan & Aggregation Benchmark...");
            int columnarRows = 100_000;
            List<Map<String, ColumnValue>> records = new ArrayList<>(columnarRows);
            for (int i = 0; i < columnarRows; i++) {
                Map<String, ColumnValue> row = new LinkedHashMap<>();
                row.put("id", ColumnValue.ofLong(i));
                row.put("category", ColumnValue.ofString("cat_" + (i % 10)));
                row.put("amount", ColumnValue.ofDouble(10.0 + (i % 100)));
                records.add(row);
            }
            engine.ingestColumnar("bench_analytics", records);

            QueryEngine queryEngine = new QueryEngine(engine, 1024);
            QueryRequest queryReq = new QueryRequest(
                    "bench_analytics",
                    List.of("COUNT(*)", "SUM(amount)", "AVG(amount)"),
                    List.of(new Predicate("amount", FilterOperator.GREATER_THAN_OR_EQUALS, ColumnValue.ofDouble(50.0))),
                    Collections.emptyList(),
                    10
            );

            // Run scan multiple times
            long scanStartNanos = System.nanoTime();
            int queryIterations = 20;
            QueryResult lastResult = null;
            for (int iter = 0; iter < queryIterations; iter++) {
                lastResult = queryEngine.execute(queryReq);
            }
            long scanTotalNanos = System.nanoTime() - scanStartNanos;

            double avgQueryDurationMs = (scanTotalNanos / (double) queryIterations) / 1_000_000.0;
            double rowsPerSec = (columnarRows * queryIterations) / (scanTotalNanos / 1_000_000_000.0);

            System.out.printf("   Scanned %d rows x %d iterations (Avg Query Time: %.2f ms | Scan Throughput: %.0f rows/sec)\n",
                    columnarRows, queryIterations, avgQueryDurationMs, rowsPerSec);
            if (lastResult != null && !lastResult.getRows().isEmpty()) {
                System.out.printf("   Sample Result Row: %s\n", lastResult.getRows().get(0));
            }
            System.out.println();
        } finally {
            engine.close();
            deleteDirectory(benchDir);
        }

        System.out.println("========================================================");
        if (anyFailures) {
            System.err.println("  Benchmark completed with FAILURES detected.");
            System.out.println("========================================================");
            System.exit(1);
        } else {
            System.out.println("  All Benchmarks Completed Successfully.");
            System.out.println("========================================================");
        }
    }

    private static void printBenchmarkSummary(String opName, int totalAttempted, long totalNanos, long failures, Collection<Long> latenciesCollection) {
        long successfulOps = totalAttempted - failures;
        double totalSec = totalNanos / 1_000_000_000.0;
        double throughput = totalSec > 0 ? (successfulOps / totalSec) : 0.0;

        if (latenciesCollection.isEmpty()) {
            System.out.printf("   %s Completed: 0 successful operations in %.2f s (0.0 IOPS) [Failures: %d]\n",
                    opName, totalSec, failures);
            System.out.println("   Latency: No successful operations to report.\n");
            return;
        }

        long[] latencies = new long[latenciesCollection.size()];
        int idx = 0;
        for (Long l : latenciesCollection) {
            latencies[idx++] = l;
        }
        Arrays.sort(latencies);

        int n = latencies.length;
        double p50 = latencies[Math.min((int) (n * 0.50), n - 1)] / 1_000_000.0;
        double p95 = latencies[Math.min((int) (n * 0.95), n - 1)] / 1_000_000.0;
        double p99 = latencies[Math.min((int) (n * 0.99), n - 1)] / 1_000_000.0;

        System.out.printf("   %s Completed: %d successful operations in %.2f s (%.1f IOPS) [Failures: %d]\n",
                opName, successfulOps, totalSec, throughput, failures);
        System.out.printf("   Latency: p50=%.3f ms | p95=%.3f ms | p99=%.3f ms\n", p50, p95, p99);
    }

    private static void deleteDirectory(File dir) {
        if (dir == null || !dir.exists()) return;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory()) deleteDirectory(f);
                else f.delete();
            }
        }
        dir.delete();
    }
}
