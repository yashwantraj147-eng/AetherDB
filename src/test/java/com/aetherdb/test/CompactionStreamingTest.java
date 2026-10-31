package com.aetherdb.test;

import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.sstable.CompactionEngine;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.sstable.SSTableWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;

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
        // Table 1 (older versions)
        File f1 = new File(tempDir.toFile(), "sstable_000001.sst");
        SSTableWriter w1 = new SSTableWriter(f1, 1, 0, 4096, 0.01);
        w1.write(List.of(
                DataRecord.put("keyA", "v1_old", 10),
                DataRecord.put("keyB", "v2_stay", 15)
        ).iterator(), 2);
        SSTableReader r1 = new SSTableReader(f1);

        // Table 2 (newer versions)
        File f2 = new File(tempDir.toFile(), "sstable_000002.sst");
        SSTableWriter w2 = new SSTableWriter(f2, 2, 0, 4096, 0.01);
        w2.write(List.of(
                DataRecord.put("keyA", "v1_newest", 25),
                DataRecord.delete("keyB", 30) // Tombstone for keyB
        ).iterator(), 2);
        SSTableReader r2 = new SSTableReader(f2);

        File target = new File(tempDir.toFile(), "sstable_000003.sst");

        // Compact with isDeepestLevel = false (tombstone should be retained)
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
    }
}
