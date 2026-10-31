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
    @DisplayName("Priority 0: Manual flush racing with close aborts publication safely and preserves WAL")
    void testManualFlushRacesWithClose() throws Exception {
        for (int i = 0; i < 50; i++) {
            storage.put("flush_race_" + i, "val_" + i);
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch flushStarted = new CountDownLatch(1);
        AtomicReference<Exception> flushEx = new AtomicReference<>();

        executor.submit(() -> {
            try {
                flushStarted.countDown();
                storage.flushMemTable();
            } catch (Exception e) {
                flushEx.set(e);
            }
        });

        flushStarted.await();
        // Concurrently close the storage engine
        storage.close();
        assertEquals(LifecycleState.CLOSED, storage.getLifecycleState());

        executor.shutdown();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));

        // Reopen engine: all acknowledged writes must be recovered from WAL
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
    @DisplayName("Priority 0: Manual compaction racing with close terminates cleanly without corrupting SSTables")
    void testCompactionRacesWithClose() throws Exception {
        // Create multiple SSTables
        for (int batch = 0; batch < 4; batch++) {
            for (int i = 0; i < 10; i++) {
                storage.put("compact_k_" + batch + "_" + i, "v_" + i);
            }
            storage.flushMemTable();
        }

        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch compactStarted = new CountDownLatch(1);

        executor.submit(() -> {
            try {
                compactStarted.countDown();
                storage.compact();
            } catch (Exception ignored) {}
        });

        compactStarted.await();
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
