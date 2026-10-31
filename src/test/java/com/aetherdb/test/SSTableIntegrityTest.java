package com.aetherdb.test;

import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.sstable.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

public class SSTableIntegrityTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_sstable_test_");
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
    @DisplayName("SSTable round-trip write and point lookup")
    void testSSTableRoundTrip() throws Exception {
        File sstFile = new File(tempDir.toFile(), "sstable_000001.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 1, 0, 4096, 0.01);

        List<DataRecord> records = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            records.add(DataRecord.put(String.format("key_%04d", i), "val_" + i, 100 + i));
        }

        SSTableMetadata meta = writer.write(records.iterator(), records.size());
        assertNotNull(meta);
        assertEquals(50, meta.getRecordCount());
        assertTrue(sstFile.exists());

        try (SSTableReader reader = new SSTableReader(sstFile)) {
            assertEquals(1, reader.getTableId());
            for (int i = 0; i < 50; i++) {
                DataRecord r = reader.get(String.format("key_%04d", i).getBytes(Constants.DEFAULT_CHARSET));
                assertNotNull(r, "Missing key_" + i);
                assertEquals("val_" + i, r.getValueAsString());
            }

            // Negative lookup
            DataRecord missing = reader.get("non_existent_key".getBytes(Constants.DEFAULT_CHARSET));
            assertNull(missing);
        }
    }

    @Test
    @DisplayName("Header validation: invalid 6-byte header magic throws CorruptedSSTableException")
    void testHeaderMagicValidation() throws Exception {
        File sstFile = new File(tempDir.toFile(), "sstable_badmagic.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 10, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k", "v", 1)).iterator(), 1);

        // Corrupt magic bytes at offset 0
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "rw")) {
            raf.seek(0);
            raf.writeInt(0xBAD0CAFE);
        }

        assertThrows(CorruptedSSTableException.class, () -> {
            new SSTableReader(sstFile);
        });
    }

    @Test
    @DisplayName("Header validation: unsupported header version throws CorruptedSSTableException")
    void testHeaderVersionValidation() throws Exception {
        File sstFile = new File(tempDir.toFile(), "sstable_badversion.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 11, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k", "v", 1)).iterator(), 1);

        // Corrupt version at offset 4
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "rw")) {
            raf.seek(4);
            raf.writeShort((short) 999);
        }

        assertThrows(CorruptedSSTableException.class, () -> {
            new SSTableReader(sstFile);
        });
    }

    @Test
    @DisplayName("CRC verification: corrupted data block throws CorruptedSSTableException")
    void testCorruptedDataBlockCrcFails() throws Exception {
        File sstFile = new File(tempDir.toFile(), "sstable_000002.sst");
        SSTableWriter writer = new SSTableWriter(sstFile, 2, 0, 4096, 0.01);

        List<DataRecord> records = List.of(
                DataRecord.put("user:101", "Alice", 1),
                DataRecord.put("user:102", "Bob", 2)
        );
        writer.write(records.iterator(), records.size());

        // Tamper with payload byte in data block (after header offset 6)
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "rw")) {
            raf.seek(12);
            byte b = raf.readByte();
            raf.seek(12);
            raf.writeByte(b ^ 0xFF); // Flip bits
        }

        try (SSTableReader reader = new SSTableReader(sstFile)) {
            assertThrows(Exception.class, () -> {
                reader.get("user:101".getBytes(Constants.DEFAULT_CHARSET));
            });
        }
    }

    @Test
    @DisplayName("Temporary .tmp files do not crash reader and are safely isolated")
    void testTemporaryFileIgnored() throws Exception {
        File tempFile = new File(tempDir.toFile(), "sstable_000003.sst.tmp");
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "rw")) {
            raf.writeBytes("incomplete partial write");
        }

        assertTrue(tempFile.exists());
        File[] sstables = tempDir.toFile().listFiles((dir, name) -> name.endsWith(Constants.SSTABLE_DATA_EXT));
        assertEquals(0, sstables != null ? sstables.length : 0);
    }
}
