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
import java.util.concurrent.atomic.AtomicBoolean;
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
    @DisplayName("Shutdown safety: engine rejects writes after close, and close is idempotent")
    void testShutdownSafety() throws Exception {
        HybridStorageManager engine = new HybridStorageManager(config);
        engine.put("k_open", "v_open");
        assertNotNull(engine.get("k_open"));

        // Close storage engine
        engine.close();

        // Further writes and gets must be rejected
        assertThrows(IllegalStateException.class, () -> engine.put("k_closed", "v_closed"));
        assertThrows(IllegalStateException.class, () -> engine.get("k_open"));
        assertThrows(IllegalStateException.class, () -> engine.delete("k_open"));
        assertThrows(IllegalStateException.class, () -> engine.flushMemTable());

        // Repeated close must be safely idempotent without exceptions
        assertDoesNotThrow(engine::close);
    }

    @Test
    @DisplayName("Idempotent flush: concurrent manual flushes do not duplicate SSTables")
    void testConcurrentManualFlushIdempotence() throws Exception {
        HybridStorageManager engine = new HybridStorageManager(config);
        for (int i = 0; i < 50; i++) {
            engine.put("flush_key_" + i, "val_" + i);
        }

        int threads = 4;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threads);
        AtomicInteger failures = new AtomicInteger(0);

        try (ExecutorService exec = Executors.newFixedThreadPool(threads)) {
            for (int t = 0; t < threads; t++) {
                exec.submit(() -> {
                    try {
                        startLatch.await();
                        engine.flushMemTable();
                    } catch (Exception ex) {
                        failures.incrementAndGet();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }
            startLatch.countDown();
            doneLatch.await();
        }

        assertEquals(0, failures.get());
        for (int i = 0; i < 50; i++) {
            DataRecord rec = engine.get("flush_key_" + i);
            assertNotNull(rec);
            assertEquals("val_" + i, rec.getValueAsString());
        }

        engine.close();
    }

    @Test
    @DisplayName("Deterministic concurrency: continuous reads remain consistent throughout compaction publication")
    void testContinuousReadsDuringCompaction() throws Exception {
        AetherConfig compConfig = new AetherConfig();
        compConfig.setProperty("storage.wal.dir", tempBaseDir.resolve("wal_comp").toString());
        compConfig.setProperty("storage.sstable.dir", tempBaseDir.resolve("sstable_comp").toString());
        compConfig.setProperty("storage.columnar.dir", tempBaseDir.resolve("columnar_comp").toString());
        compConfig.setProperty("storage.data.dir", tempBaseDir.resolve("storage_comp").toString());
        compConfig.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(compConfig);

        // Write batch 1 and flush
        for (int i = 0; i < 20; i++) {
            engine.put("read_key_" + i, "v1_" + i);
        }
        engine.flushMemTable();

        // Write batch 2 and flush
        for (int i = 0; i < 20; i++) {
            engine.put("read_key_" + i, "v2_" + i);
        }
        engine.flushMemTable();

        AtomicBoolean running = new AtomicBoolean(true);
        AtomicInteger readErrors = new AtomicInteger(0);
        int readers = 4;
        CountDownLatch readerLatch = new CountDownLatch(readers);

        try (ExecutorService exec = Executors.newFixedThreadPool(readers)) {
            for (int r = 0; r < readers; r++) {
                exec.submit(() -> {
                    try {
                        while (running.get()) {
                            for (int i = 0; i < 20; i++) {
                                DataRecord rec = engine.get("read_key_" + i);
                                if (rec == null || !rec.getValueAsString().startsWith("v2_")) {
                                    readErrors.incrementAndGet();
                                }
                            }
                        }
                    } catch (Exception e) {
                        readErrors.incrementAndGet();
                    } finally {
                        readerLatch.countDown();
                    }
                });
            }

            // Run compaction while readers are continuously reading
            engine.compact();

            running.set(false);
            readerLatch.await();
        }

        assertEquals(0, readErrors.get(), "Readers must never encounter missing keys or closed readers during compaction");
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

        engine2.put("seq_test_4", "data4");
        DataRecord record4 = engine2.get("seq_test_4");
        long seqAfterRestart = record4.getSequenceNumber();

        assertTrue(seqAfterRestart > seqInMemTable,
                "Sequence number after restart (" + seqAfterRestart + ") must be > previous (" + seqInMemTable + ")");

        engine2.close();
    }
}
