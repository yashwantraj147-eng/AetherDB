package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.engine.HybridStorageManager;
import com.aetherdb.storage.sstable.CompactionEngine;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.sstable.SSTableWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class CompactionStreamingTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_compaction_test_");
    }

    @AfterEach
    void tearDown() throws Exception {
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Streaming compaction resolves duplicate keys keeping highest sequence number")
    void testCompactionPreservesNewestSequence() throws Exception {
        File f1 = new File(tempDir.toFile(), "sstable_000001.sst");
        SSTableWriter w1 = new SSTableWriter(f1, 1, 0, 4096, 0.01);
        w1.write(List.of(
                DataRecord.put("keyA", "v1_old", 10),
                DataRecord.put("keyB", "v2_stay", 15)
        ).iterator(), 2);
        SSTableReader r1 = new SSTableReader(f1);

        File f2 = new File(tempDir.toFile(), "sstable_000002.sst");
        SSTableWriter w2 = new SSTableWriter(f2, 2, 0, 4096, 0.01);
        w2.write(List.of(
                DataRecord.put("keyA", "v1_newest", 25),
                DataRecord.delete("keyB", 30) // Tombstone for keyB
        ).iterator(), 2);
        SSTableReader r2 = new SSTableReader(f2);

        File target = new File(tempDir.toFile(), "sstable_000003.sst");

        // Compact with canPruneTombstones = false (tombstone must be retained)
        SSTableReader compacted = CompactionEngine.compact(
                List.of(r1, r2),
                target,
                3,
                1,
                4096,
                0.01,
                false
        );

        assertNotNull(compacted);
        assertEquals(3, compacted.getTableId());

        // keyA must be newest version
        DataRecord aRec = compacted.get("keyA".getBytes(Constants.DEFAULT_CHARSET));
        assertNotNull(aRec);
        assertEquals("v1_newest", aRec.getValueAsString());
        assertEquals(25, aRec.getSequenceNumber());

        // keyB must be tombstone retained
        DataRecord bRec = compacted.get("keyB".getBytes(Constants.DEFAULT_CHARSET));
        assertNotNull(bRec);
        assertTrue(bRec.isTombstone());
        assertEquals(30, bRec.getSequenceNumber());

        compacted.close();
        r1.close();
        r2.close();
    }

    @Test
    @DisplayName("Compaction failure before publication leaves all input SSTables valid and readable")
    void testCompactionFailureLeavesInputTablesIntact() throws Exception {
        File f1 = new File(tempDir.toFile(), "sstable_000001.sst");
        SSTableWriter w1 = new SSTableWriter(f1, 1, 0, 4096, 0.01);
        w1.write(List.of(DataRecord.put("k1", "v1", 10)).iterator(), 1);
        SSTableReader r1 = new SSTableReader(f1);

        // Target path is invalid/unwritable to trigger IO failure
        File invalidTarget = new File("/non_existent_dir_aether/target.sst");

        assertThrows(IOException.class, () -> {
            CompactionEngine.compact(List.of(r1), invalidTarget, 99, 1, 4096, 0.01, false);
        });

        // Input reader r1 must remain completely unaffected, open, and readable
        assertFalse(r1.isClosed());
        DataRecord rec = r1.get("k1".getBytes(Constants.DEFAULT_CHARSET));
        assertNotNull(rec);
        assertEquals("v1", rec.getValueAsString());

        r1.close();
    }

    @Test
    @DisplayName("Tombstone resurrection prevention: unflushed MemTable prevents premature tombstone pruning")
    void testTombstoneResurrectionPrevention() throws Exception {
        AetherConfig cfg = new AetherConfig();
        cfg.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        cfg.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        cfg.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        cfg.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        cfg.setProperty("lsm.compaction.min.threshold", "10"); // Disable auto-compaction
        cfg.validateAndCreateDirectories();

        HybridStorageManager engine = new HybridStorageManager(cfg);

        // 1. Initial write
        engine.put("resurrect_key", "old_value_v1");
        engine.flushMemTable(); // Flush to SSTable 1

        // 2. Put older write into SSTable 2
        engine.put("other_key", "other_val");
        engine.flushMemTable(); // Flush to SSTable 2

        // 3. Put DELETE into active memtable and flush to SSTable 3
        engine.delete("resurrect_key");
        engine.flushMemTable(); // Flush tombstone to SSTable 3

        // Verify key is deleted
        assertNull(engine.get("resurrect_key"));

        // 4. Run compaction across all SSTables
        engine.compact();

        // Key must still be deleted after compaction
        assertNull(engine.get("resurrect_key"), "Deleted record must not resurrect after compaction");

        engine.close();
    }
}
