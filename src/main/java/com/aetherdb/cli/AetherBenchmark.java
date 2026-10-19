package com.aetherdb.cli;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.storage.engine.HybridStorageManager;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-concurrency performance benchmark harness for AetherDB storage engine.
 */
public class AetherBenchmark {

    public static void main(String[] args) throws Exception {
        int threadCount = args.length > 0 ? Integer.parseInt(args[0]) : 8;
        int operationsPerThread = args.length > 1 ? Integer.parseInt(args[1]) : 50000;
        int totalOps = threadCount * operationsPerThread;

        System.out.println("========================================================");
        System.out.println("  AetherDB High-Concurrency Performance Benchmark");
        System.out.println("  Concurrency: " + threadCount + " threads | Total Operations: " + totalOps);
        System.out.println("========================================================\n");

        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", "./data/bench/wal");
        config.setProperty("storage.sstable.dir", "./data/bench/sstable");
        config.setProperty("storage.columnar.dir", "./data/bench/columnar");
        config.setProperty("storage.data.dir", "./data/bench/storage");
        config.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(config);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount,
                Thread.ofVirtual().name("bench-worker-", 1).factory()
        );

        // Benchmark 1: Sequential & Concurrent Puts
        System.out.println(">> Executing " + totalOps + " concurrent PUT operations...");
        CountDownLatch putLatch = new CountDownLatch(threadCount);
        long startPut = System.currentTimeMillis();

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key:" + threadId + ":" + i;
                        String val = "payload_data_value_string_" + i;
                        engine.put(key, val);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    putLatch.countDown();
                }
            });
        }

        putLatch.await();
        long putDurationMs = Math.max(1, System.currentTimeMillis() - startPut);
        double putIops = (double) totalOps / (putDurationMs / 1000.0);
        System.out.printf(">> PUT Complete: %d ops in %d ms (%.2f IOPS)\n\n", totalOps, putDurationMs, putIops);

        // Benchmark 2: Concurrent Gets
        System.out.println(">> Executing " + totalOps + " concurrent GET operations...");
        CountDownLatch getLatch = new CountDownLatch(threadCount);
        long startGet = System.currentTimeMillis();
        AtomicLong foundCount = new AtomicLong(0);

        for (int t = 0; t < threadCount; t++) {
            final int threadId = t;
            executor.submit(() -> {
                try {
                    for (int i = 0; i < operationsPerThread; i++) {
                        String key = "key:" + threadId + ":" + i;
                        if (engine.get(key) != null) {
                            foundCount.incrementAndGet();
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    getLatch.countDown();
                }
            });
        }

        getLatch.await();
        long getDurationMs = Math.max(1, System.currentTimeMillis() - startGet);
        double getIops = (double) totalOps / (getDurationMs / 1000.0);
        System.out.printf(">> GET Complete: %d ops in %d ms (%.2f IOPS) - Hit Ratio: %.2f%%\n\n",
                totalOps, getDurationMs, getIops, (foundCount.get() * 100.0 / totalOps));

        executor.shutdown();
        engine.close();

        System.out.println("========================================================");
        System.out.println("  Benchmark Finished Successfully.");
        System.out.println("========================================================");
    }
}
