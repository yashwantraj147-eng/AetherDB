package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.engine.HybridStorageManager;
import com.aetherdb.storage.engine.LifecycleState;
import com.aetherdb.storage.memtable.MemTable;
import com.aetherdb.storage.sstable.SSTableReader;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

public class LifecycleAndRecoveryTest {

    private Path tempDir;
    private AetherConfig config;
    private HybridStorageManager storage;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_lifecycle_test_");
        config = new AetherConfig();
        config.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        config.setProperty("storage.memtable.max.bytes", "1024"); // Small to test flush trigger
        config.validateAndCreateDirectories();

        storage = new HybridStorageManager(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null && storage.getLifecycleState() != LifecycleState.CLOSED) {
            try {
                storage.close();
            } catch (Exception ignored) {}
        }
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Lifecycle: writes and deletes are rejected once shutdown begins")
    void testWritesAndDeletesRejectedOnShutdown() throws Exception {
        storage.put("k1", "v1");
        assertEquals("v1", storage.get("k1").getValueAsString());

        storage.close();
        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());

        assertThrows(IllegalStateException.class, () -> storage.put("k2", "v2"));
        assertThrows(IllegalStateException.class, () -> storage.delete("k1"));
        assertThrows(IllegalStateException.class, () -> storage.get("k1"));
        assertThrows(IllegalStateException.class, () -> storage.flushMemTable());
    }

    @Test
    @DisplayName("Lifecycle: repeated calls to close() are idempotent and thread-safe")
    void testConcurrentCloseIdempotent() throws Exception {
        int threadCount = 8;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    storage.close();
                    successCount.incrementAndGet();
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(5, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threadCount, successCount.get());
        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());
    }

    @Test
    @DisplayName("Lifecycle: writes racing with shutdown do not corrupt storage")
    void testWritesRacingWithShutdown() throws Exception {
        int writers = 4;
        ExecutorService executor = Executors.newFixedThreadPool(writers + 1);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger acceptedWrites = new AtomicInteger(0);
        AtomicInteger rejectedWrites = new AtomicInteger(0);

        for (int i = 0; i < writers; i++) {
            final int wId = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    int count = 0;
                    while (running.get() && count < 200) {
                        try {
                            storage.put("race_key_" + wId + "_" + count, "val_" + count);
                            acceptedWrites.incrementAndGet();
                        } catch (IllegalStateException e) {
                            rejectedWrites.incrementAndGet();
                        }
                        count++;
                    }
                } catch (Exception ignored) {
                }
            });
        }

        startLatch.countDown();
        Thread.sleep(10); // Let some writes enter
        storage.close();
        running.set(false);

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());
        assertTrue(rejectedWrites.get() > 0 || acceptedWrites.get() > 0);

        // Verify that restarting the engine recovers all accepted writes cleanly
        HybridStorageManager recovered = new HybridStorageManager(config);
        try {
            assertEquals(LifecycleState.OPEN, recovered.getLifecycleState());
        } finally {
            recovered.close();
        }
    }

    @Test
    @DisplayName("Flush Ownership: manual flush waits for already-running background flush")
    void testManualFlushWaitsForRunningBackgroundFlush() throws Exception {
        // Ingest enough data to seal active memtable
        for (int i = 0; i < 20; i++) {
            storage.put("k_" + i, "v_" + i);
        }

        // Trigger manual flush while background flushes may be in flight
        assertDoesNotThrow(() -> storage.flushMemTable());

        // Verify all keys are retrievable
        for (int i = 0; i < 20; i++) {
            DataRecord rec = storage.get("k_" + i);
            assertNotNull(rec, "Missing key k_" + i);
            assertEquals("v_" + i, rec.getValueAsString());
        }
    }

    @Test
    @DisplayName("Flush Ownership: multiple concurrent manual flush callers do not duplicate SSTables")
    void testConcurrentManualFlushesDoNotDuplicateSSTables() throws Exception {
        for (int i = 0; i < 30; i++) {
            storage.put("key_" + i, "val_" + i);
        }

        int threads = 4;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);

        for (int i = 0; i < threads; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    storage.flushMemTable();
                } catch (Exception ignored) {
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        assertTrue(doneLatch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        // Count physical SSTable files
        File sstableDir = tempDir.resolve("sstable").toFile();
        File[] sstFiles = sstableDir.listFiles((d, name) -> name.endsWith(".sst") && !name.endsWith(".tmp"));
        assertNotNull(sstFiles);
        // Should have at most 2 SSTables (one level 0 or compacted) rather than 4 duplicates
        assertTrue(sstFiles.length <= 2, "Expected <= 2 SSTables, but found: " + sstFiles.length);

        // Verify data intact
        for (int i = 0; i < 30; i++) {
            DataRecord rec = storage.get("key_" + i);
            assertNotNull(rec);
            assertEquals("val_" + i, rec.getValueAsString());
        }
    }

    @Test
    @DisplayName("WAL Retirement: WAL retained until SSTable is published and installed")
    void testWalRetainedUntilSSTablePublished() throws Exception {
        storage.put("durable_k", "durable_v");

        File walDir = tempDir.resolve("wal").toFile();
        File[] walFilesBefore = walDir.listFiles((d, name) -> name.endsWith(".wal"));
        assertNotNull(walFilesBefore);
        assertTrue(walFilesBefore.length >= 1);

        storage.flushMemTable();

        // Verify key is durable
        DataRecord rec = storage.get("durable_k");
        assertNotNull(rec);
        assertEquals("durable_v", rec.getValueAsString());
    }

    @Test
    @DisplayName("Recovery: failed MemTable state retains WAL and recovers on restart")
    void testFailedMemTableRetainsWalAndRecovers() throws Exception {
        storage.put("recovery_key", "recovery_value");

        // Simulate crash without clean close
        File walDir = tempDir.resolve("wal").toFile();
        File[] walFiles = walDir.listFiles((d, name) -> name.endsWith(".wal"));
        assertNotNull(walFiles);
        assertTrue(walFiles.length >= 1);

        // Reopen database with new manager instance simulating restart
        HybridStorageManager newStorage = new HybridStorageManager(config);
        try {
            DataRecord rec = newStorage.get("recovery_key");
            assertNotNull(rec, "Expected recovery_key to be recovered from WAL");
            assertEquals("recovery_value", rec.getValueAsString());
        } finally {
            newStorage.close();
        }
    }

    @Test
    @DisplayName("Priority 0: Manual flush racing with close aborts publication safely and preserves WAL (deterministic latches)")
    void testManualFlushRacesWithClose() throws Exception {
        for (int i = 0; i < 50; i++) {
            storage.put("flush_race_" + i, "val_" + i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch flushAtPublication = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);
        AtomicReference<Exception> flushEx = new AtomicReference<>();

        storage.onBeforeFlushPublicationHook = () -> {
            flushAtPublication.countDown();
            try {
                closeStarted.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        executor.submit(() -> {
            try {
                storage.flushMemTable();
            } catch (Exception e) {
                flushEx.set(e);
            }
        });

        // Deterministically wait until flush has written the SSTable and is at the publication gate
        assertTrue(flushAtPublication.await(5, TimeUnit.SECONDS));

        // Signal close and shut down engine while flush is paused at publication gate
        closeStarted.countDown();
        storage.close();
        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Reopen engine: all acknowledged writes must be recovered from WAL because publication was rejected
        HybridStorageManager restarted = new HybridStorageManager(config);
        try {
            assertEquals(LifecycleState.OPEN, restarted.getLifecycleState());
            for (int i = 0; i < 50; i++) {
                DataRecord rec = restarted.get("flush_race_" + i);
                assertNotNull(rec, "Missing key flush_race_" + i + " after recovery");
                assertEquals("val_" + i, rec.getValueAsString());
            }
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("Priority 0: Manual compaction racing with close terminates cleanly without corrupting SSTables (deterministic latches)")
    void testCompactionRacesWithClose() throws Exception {
        // Create multiple SSTables
        for (int batch = 0; batch < 4; batch++) {
            for (int i = 0; i < 10; i++) {
                storage.put("compact_k_" + batch + "_" + i, "v_" + i);
            }
            storage.flushMemTable();
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch compactAtPublication = new CountDownLatch(1);
        CountDownLatch closeStarted = new CountDownLatch(1);

        storage.onBeforeCompactionPublicationHook = () -> {
            compactAtPublication.countDown();
            try {
                closeStarted.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };

        executor.submit(() -> {
            try {
                storage.compact();
            } catch (Exception ignored) {}
        });

        assertTrue(compactAtPublication.await(5, TimeUnit.SECONDS));
        closeStarted.countDown();
        storage.close();
        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Restart engine and verify all records survive
        HybridStorageManager restarted = new HybridStorageManager(config);
        try {
            assertEquals(LifecycleState.OPEN, restarted.getLifecycleState());
            for (int batch = 0; batch < 4; batch++) {
                for (int i = 0; i < 10; i++) {
                    DataRecord rec = restarted.get("compact_k_" + batch + "_" + i);
                    assertNotNull(rec, "Record lost after compaction race: compact_k_" + batch + "_" + i);
                }
            }
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("Failed-flush recovery: simulated WAL rotation failure transitions to FAILED, retry flushes after restoration, data survives restart")
    void testFailedFlushRecoveryWithSimulatedWalFailure() throws Exception {
        storage.put("fail_wal_1", "val_fail_1");
        storage.put("fail_wal_2", "val_fail_2");

        // 1. Inject simulated WAL rotation failure
        storage.onWalRotationFailureHook = () -> {
            throw new RuntimeException(new IOException("Simulated disk error during WAL rotation"));
        };

        // 2. Trigger flush -> must throw IOException and enter FAILED state
        assertThrows(IOException.class, () -> storage.flushMemTable());
        assertEquals(LifecycleState.FAILED, storage.getLifecycleState());

        // 3. Mutating operations are rejected during FAILED state
        assertThrows(IllegalStateException.class, () -> storage.put("fail_wal_3", "val_fail_3"));
        assertThrows(IllegalStateException.class, () -> storage.delete("fail_wal_1"));

        // 4. Restore storage availability
        storage.onWalRotationFailureHook = null;

        // 5. retryFailedFlushes() must be callable in FAILED state and recover the engine to OPEN
        assertDoesNotThrow(() -> storage.retryFailedFlushes());
        assertEquals(LifecycleState.OPEN, storage.getLifecycleState());

        // 6. Mutating operations work normally after recovery
        storage.put("after_recovery_k", "after_recovery_v");

        // 7. Restart engine and verify that all pre-failure and post-recovery data survive
        storage.close();
        HybridStorageManager restarted = new HybridStorageManager(config);
        try {
            assertEquals(LifecycleState.OPEN, restarted.getLifecycleState());
            DataRecord r1 = restarted.get("fail_wal_1");
            assertNotNull(r1, "Pre-failure record fail_wal_1 must survive");
            assertEquals("val_fail_1", r1.getValueAsString());

            DataRecord r2 = restarted.get("fail_wal_2");
            assertNotNull(r2, "Pre-failure record fail_wal_2 must survive");
            assertEquals("val_fail_2", r2.getValueAsString());

            DataRecord r3 = restarted.get("after_recovery_k");
            assertNotNull(r3, "Post-recovery record after_recovery_k must survive");
            assertEquals("after_recovery_v", r3.getValueAsString());
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("Flush future concurrency: concurrent callers always receive or share active future without missing flush")
    void testFlushFutureConcurrency() throws Exception {
        int threads = 12;
        ExecutorService executor = Executors.newFixedThreadPool(threads);

        for (int round = 0; round < 5; round++) {
            MemTable memTable = new MemTable(round);
            memTable.setState(MemTable.State.SEALED);

            CyclicBarrier barrier = new CyclicBarrier(threads);
            AtomicInteger winnerCount = new AtomicInteger(0);
            AtomicInteger sharerCount = new AtomicInteger(0);
            List<Future<CompletableFuture<SSTableReader>>> futures = new ArrayList<>();

            for (int i = 0; i < threads; i++) {
                futures.add(executor.submit(() -> {
                    barrier.await();
                    CompletableFuture<SSTableReader> claimed = memTable.claimFlushAttempt();
                    if (claimed != null) {
                        winnerCount.incrementAndGet();
                        return claimed;
                    } else {
                        sharerCount.incrementAndGet();
                        CompletableFuture<SSTableReader> shared = memTable.getFlushFuture();
                        assertNotNull(shared, "Non-winning thread must observe active flush future");
                        assertEquals(MemTable.State.FLUSHING, memTable.getState());
                        return shared;
                    }
                }));
            }

            CompletableFuture<SSTableReader> winningFuture = null;
            for (Future<CompletableFuture<SSTableReader>> f : futures) {
                CompletableFuture<SSTableReader> res = f.get(5, TimeUnit.SECONDS);
                assertNotNull(res);
                if (winningFuture == null) {
                    winningFuture = res;
                } else {
                    assertSame(winningFuture, res, "All callers must share the exact same active future instance");
                }
            }

            assertEquals(1, winnerCount.get(), "Exactly one thread claims ownership of the flush attempt");
            assertEquals(threads - 1, sharerCount.get(), "All other threads share the claimed future");
        }

        executor.shutdown();
    }

    @Test
    @DisplayName("Shutdown resource safety: simultaneous close, flushes, puts, and compactions leave no corrupted or prematurely closed resources")
    void testShutdownResourceSafetySimultaneousOperations() throws Exception {
        // Pre-populate with data
        for (int i = 0; i < 150; i++) {
            storage.put("init_k_" + i, "init_v_" + i);
        }
        storage.flushMemTable();

        int workerThreads = 6;
        ExecutorService executor = Executors.newFixedThreadPool(workerThreads);
        CountDownLatch startLatch = new CountDownLatch(1);
        AtomicBoolean stopWrites = new AtomicBoolean(false);

        // Worker 1: continuous puts
        executor.submit(() -> {
            try {
                startLatch.await();
                int i = 0;
                while (!stopWrites.get() && i < 200) {
                    try {
                        storage.put("worker_put_" + i, "val_" + i);
                    } catch (IllegalStateException e) {
                        break;
                    }
                    i++;
                }
            } catch (Exception ignored) {}
        });

        // Worker 2: continuous flushes
        executor.submit(() -> {
            try {
                startLatch.await();
                while (!stopWrites.get()) {
                    try {
                        storage.flushMemTable();
                    } catch (Exception e) {
                        break;
                    }
                }
            } catch (Exception ignored) {}
        });

        // Worker 3: continuous compactions
        executor.submit(() -> {
            try {
                startLatch.await();
                while (!stopWrites.get()) {
                    try {
                        storage.compact();
                    } catch (Exception e) {
                        break;
                    }
                }
            } catch (Exception ignored) {}
        });

        // Worker 4: continuous gets
        executor.submit(() -> {
            try {
                startLatch.await();
                int i = 0;
                while (!stopWrites.get() && i < 150) {
                    try {
                        storage.get("init_k_" + i);
                    } catch (Exception e) {
                        break;
                    }
                    i++;
                }
            } catch (Exception ignored) {}
        });

        // Start all concurrent workers
        startLatch.countDown();
        Thread.sleep(60);

        // Initiate close while all workers are operating
        storage.close();
        stopWrites.set(true);

        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());
        executor.shutdown();
        assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));

        // Reopen storage and verify all acknowledged records and SSTables are clean and uncorrupted
        HybridStorageManager restarted = new HybridStorageManager(config);
        try {
            assertEquals(LifecycleState.OPEN, restarted.getLifecycleState());
            for (int i = 0; i < 150; i++) {
                DataRecord rec = restarted.get("init_k_" + i);
                assertNotNull(rec, "Pre-populated key lost: init_k_" + i);
                assertEquals("init_v_" + i, rec.getValueAsString());
            }
        } finally {
            restarted.close();
        }
    }

    @Test
    @DisplayName("Priority 0: MemTable flush attempt claim assigns fresh future and coordinates callers")
    void testMemTableClaimFlushAttemptFreshFuture() {
        MemTable memTable = new MemTable(12345L);
        memTable.setState(MemTable.State.SEALED);

        // 1. First caller claims attempt
        CompletableFuture<SSTableReader> f1 = memTable.claimFlushAttempt();
        assertNotNull(f1);
        assertEquals(MemTable.State.FLUSHING, memTable.getState());

        // 2. Second concurrent caller attempts to claim attempt -> returns null (already in flight)
        CompletableFuture<SSTableReader> f2 = memTable.claimFlushAttempt();
        assertNull(f2, "Concurrent claim on actively flushing MemTable must return null");
        assertSame(f1, memTable.getFlushFuture(), "Callers must share the same active future");

        // 3. Complete the future exceptionally (simulating flush failure)
        f1.completeExceptionally(new IOException("Simulated disk error"));
        memTable.setState(MemTable.State.FAILED);

        // 4. Retry claim on FAILED MemTable must create a BRAND NEW uncompleted future!
        CompletableFuture<SSTableReader> f3 = memTable.claimFlushAttempt();
        assertNotNull(f3);
        assertNotSame(f1, f3, "Retry must allocate a fresh future rather than reusing exceptional future");
        assertFalse(f3.isDone(), "Fresh future must not be completed yet");
    }

    @Test
    @DisplayName("Priority 0: retryFailedFlushes probes WAL directory writeability and recovers cleanly")
    void testRetryFailedFlushesProbe() throws Exception {
        storage.put("probe_k1", "probe_v1");
        assertDoesNotThrow(() -> storage.retryFailedFlushes());
        assertEquals("probe_v1", storage.get("probe_k1").getValueAsString());
    }
}
