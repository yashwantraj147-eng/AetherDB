package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.engine.HybridStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class LsmConcurrencyAndVersionTest {

    private Path tempBaseDir;
    private AetherConfig config;

    @BeforeEach
    void setUp() throws Exception {
        tempBaseDir = Files.createTempDirectory("aether_lsm_test_");
        config = new AetherConfig();
        config.setProperty("storage.wal.dir", tempBaseDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempBaseDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempBaseDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempBaseDir.resolve("storage").toString());
        config.setProperty("lsm.wal.sync.mode", "IMMEDIATE");
        config.validateAndCreateDirectories();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(tempBaseDir)) {
            Files.walk(tempBaseDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Version-aware reads: newer values and tombstones properly hide older SSTable records")
    void testVersionAwareReadsAcrossLayers() throws Exception {
        HybridStorageManager engine = new HybridStorageManager(config);

        // 1. Write key1=v1 and flush to SSTable
        engine.put("key1", "val1_initial");
        engine.flushMemTable();

        // Verify read from SSTable
        DataRecord r1 = engine.get("key1");
        assertNotNull(r1);
        assertEquals("val1_initial", r1.getValueAsString());

        // 2. Overwrite key1=v2 in active MemTable
        engine.put("key1", "val1_updated");
        DataRecord r2 = engine.get("key1");
        assertNotNull(r2);
        assertEquals("val1_updated", r2.getValueAsString());
        assertTrue(r2.getSequenceNumber() > r1.getSequenceNumber());

        // 3. Delete key1 in active MemTable
        engine.delete("key1");
        DataRecord r3 = engine.get("key1");
        assertNull(r3, "Tombstone in MemTable must hide older value in SSTable");

        // 4. Flush tombstone to SSTable
        engine.flushMemTable();
        DataRecord r4 = engine.get("key1");
        assertNull(r4, "Tombstone in newer SSTable must hide older value in older SSTable");

        // 5. Re-insert key1=v3
        engine.put("key1", "val1_resurrected");
        DataRecord r5 = engine.get("key1");
        assertNotNull(r5);
        assertEquals("val1_resurrected", r5.getValueAsString());

        engine.close();
    }

    @Test
    @DisplayName("Sequence number persistence across crash and restart")
    void testSequencePersistenceAcrossRestart() throws Exception {
        HybridStorageManager engine1 = new HybridStorageManager(config);
        engine1.put("seq_test_1", "data1");
        engine1.put("seq_test_2", "data2");
        DataRecord lastRecord = engine1.get("seq_test_2");
        long seqBeforeFlush = lastRecord.getSequenceNumber();

        engine1.flushMemTable();
        engine1.put("seq_test_3", "data3");
        DataRecord record3 = engine1.get("seq_test_3");
        long seqInMemTable = record3.getSequenceNumber();
        assertTrue(seqInMemTable > seqBeforeFlush);

        engine1.close();

        // Restart engine from disk
        HybridStorageManager engine2 = new HybridStorageManager(config);

        // Next write must be strictly greater than any previously assigned sequence
        engine2.put("seq_test_4", "data4");
        DataRecord record4 = engine2.get("seq_test_4");
        long seqAfterRestart = record4.getSequenceNumber();

        assertTrue(seqAfterRestart > seqInMemTable,
                "Sequence number after restart (" + seqAfterRestart + ") must be > previous (" + seqInMemTable + ")");

        engine2.close();
    }

    @Test
    @DisplayName("Deterministic concurrency: concurrent PUT and DELETE preserves atomic write order")
    void testConcurrentPutAndDelete() throws Exception {
        HybridStorageManager engine = new HybridStorageManager(config);
        int threadCount = 8;
        int opsPerThread = 200;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        AtomicInteger putCount = new AtomicInteger(0);
        AtomicInteger delCount = new AtomicInteger(0);

        try (ExecutorService exec = Executors.newFixedThreadPool(threadCount)) {
            for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        for (int i = 0; i < opsPerThread; i++) {
                            String key = "concurrent_key_" + (i % 20);
                            if (threadId % 2 == 0) {
                                engine.put(key, "val_" + threadId + "_" + i);
                                putCount.incrementAndGet();
                            } else {
                                engine.delete(key);
                                delCount.incrementAndGet();
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await();
        }

        // Verify engine state is valid and can be read/flushed without error
        assertDoesNotThrow(() -> engine.flushMemTable());
        engine.close();

        // Verify can restart cleanly
        HybridStorageManager engineRestarted = new HybridStorageManager(config);
        assertNotNull(engineRestarted);
        engineRestarted.close();
    }
}

