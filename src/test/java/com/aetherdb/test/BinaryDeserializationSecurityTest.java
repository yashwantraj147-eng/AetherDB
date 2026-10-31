package com.aetherdb.test;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.CloseableIterator;
import com.aetherdb.common.Constants;
import com.aetherdb.model.*;
import com.aetherdb.storage.sstable.CompactionEngine;
import com.aetherdb.storage.sstable.CorruptedSSTableException;
import com.aetherdb.storage.sstable.SSTableReader;
import com.aetherdb.storage.sstable.SSTableWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class BinaryDeserializationSecurityTest {

    private Path tempDir;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_deserialization_test_");
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
    @DisplayName("Schema Deserialization: huge column count rejected with CorruptedSchemaException")
    void testHugeColumnCountRejected() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        BinaryUtils.writeString(buf, "test_table");
        BinaryUtils.writeVarInt(buf, 500_000); // Exceeds MAX_COLUMNS (10,000)
        buf.flip();

        assertThrows(CorruptedSchemaException.class, () -> Schema.deserialize(buf));
    }

    @Test
    @DisplayName("Schema Deserialization: negative column count rejected with CorruptedSchemaException")
    void testNegativeColumnCountRejected() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        BinaryUtils.writeString(buf, "test_table");
        BinaryUtils.writeVarInt(buf, -5);
        buf.flip();

        assertThrows(CorruptedSchemaException.class, () -> Schema.deserialize(buf));
    }

    @Test
    @DisplayName("Schema Deserialization: duplicate column names rejected with CorruptedSchemaException")
    void testDuplicateColumnNamesRejected() {
        ByteBuffer buf = ByteBuffer.allocate(128);
        BinaryUtils.writeString(buf, "dup_table");
        BinaryUtils.writeVarInt(buf, 2);
        BinaryUtils.writeString(buf, "col_a");
        buf.put(ColumnType.INT32.getTypeId());
        BinaryUtils.writeString(buf, "col_a"); // Duplicate
        buf.put(ColumnType.STRING.getTypeId());
        buf.flip();

        assertThrows(CorruptedSchemaException.class, () -> Schema.deserialize(buf));
    }

    @Test
    @DisplayName("Schema Deserialization: invalid column type ID rejected with CorruptedSchemaException")
    void testInvalidColumnTypeIdRejected() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        BinaryUtils.writeString(buf, "bad_type_table");
        BinaryUtils.writeVarInt(buf, 1);
        BinaryUtils.writeString(buf, "col1");
        buf.put((byte) 99); // Unsupported type ID
        buf.flip();

        assertThrows(CorruptedSchemaException.class, () -> Schema.deserialize(buf));
    }

    @Test
    @DisplayName("Schema Deserialization: truncated column name rejected with CorruptedSchemaException")
    void testTruncatedColumnNameRejected() {
        ByteBuffer buf = ByteBuffer.allocate(64);
        BinaryUtils.writeString(buf, "trunc_table");
        BinaryUtils.writeVarInt(buf, 1);
        BinaryUtils.writeVarInt(buf, 100); // Claims 100 bytes length
        buf.put("abc".getBytes()); // Only provides 3 bytes
        buf.flip();

        assertThrows(CorruptedSchemaException.class, () -> Schema.deserialize(buf));
    }

    @Test
    @DisplayName("SSTable Offsets: arithmetic overflow in offset calculations rejected")
    void testSSTableOffsetOverflowRejected() throws Exception {
        File sstFile = tempDir.resolve("overflow.sst").toFile();
        SSTableWriter writer = new SSTableWriter(sstFile, 1, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k", "v", 1)).iterator(), 1);
        assertTrue(sstFile.exists());

        // Corrupt index offset in footer to Long.MAX_VALUE - 5
        long footerLenOffset = sstFile.length() - 4;
        int footerLen;
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "rw")) {
            raf.seek(footerLenOffset);
            footerLen = raf.readInt();

            long metadataOffset = footerLenOffset - footerLen;
            raf.seek(metadataOffset);
            raf.writeLong(1); // tableId
            raf.writeInt(0); // level
            raf.writeLong(1); // recordCount
            raf.writeLong(1); // minSeq
            raf.writeLong(1); // maxSeq
            raf.writeLong(Long.MAX_VALUE - 10); // indexOffset near Long.MAX_VALUE causing overflow
            raf.writeInt(100); // indexLength
        }

        assertThrows(CorruptedSSTableException.class, () -> new SSTableReader(sstFile));
    }

    @Test
    @DisplayName("SSTable Offsets: overlapping index and Bloom regions rejected")
    void testOverlappingIndexAndBloomRegionsRejected() throws Exception {
        File sstFile = tempDir.resolve("overlap.sst").toFile();
        SSTableWriter writer = new SSTableWriter(sstFile, 2, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k", "v", 1)).iterator(), 1);

        long footerLenOffset = sstFile.length() - 4;
        try (RandomAccessFile raf = new RandomAccessFile(sstFile, "rw")) {
            raf.seek(footerLenOffset);
            int footerLen = raf.readInt();
            long metadataOffset = footerLenOffset - footerLen;

            raf.seek(metadataOffset);
            raf.writeLong(2); // tableId
            raf.writeInt(0); // level
            raf.writeLong(1); // recordCount
            raf.writeLong(1); // minSeq
            raf.writeLong(1); // maxSeq
            raf.writeLong(10); // indexOffset
            raf.writeInt(500); // indexLength (extends to 510)
            raf.writeLong(200); // bloomOffset (starts at 200 < 510 -> overlaps index!)
            raf.writeInt(100); // bloomLength
        }

        assertThrows(CorruptedSSTableException.class, () -> new SSTableReader(sstFile));
    }

    @Test
    @DisplayName("SSTable Iterator: fully consumed iterator releases reader reference")
    void testIteratorFullyConsumedReleasesRef() throws Exception {
        File sstFile = tempDir.resolve("iter_full.sst").toFile();
        SSTableWriter writer = new SSTableWriter(sstFile, 3, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k1", "v1", 1), DataRecord.put("k2", "v2", 2)).iterator(), 2);

        SSTableReader reader = new SSTableReader(sstFile);
        assertEquals(1, reader.getRefCount());

        CloseableIterator<DataRecord> it = reader.iterator();
        assertEquals(2, reader.getRefCount()); // Iterator acquired reference

        assertTrue(it.hasNext());
        assertEquals("k1", it.next().getKeyAsString());
        assertTrue(it.hasNext());
        assertEquals("k2", it.next().getKeyAsString());
        assertFalse(it.hasNext()); // Consumed to end -> auto-closed

        assertEquals(1, reader.getRefCount()); // Released
        reader.close();
    }

    @Test
    @DisplayName("SSTable Iterator: partially consumed iterator releases reference on close()")
    void testIteratorPartiallyConsumedReleasesRefOnClose() throws Exception {
        File sstFile = tempDir.resolve("iter_partial.sst").toFile();
        SSTableWriter writer = new SSTableWriter(sstFile, 4, 0, 4096, 0.01);
        writer.write(List.of(DataRecord.put("k1", "v1", 1), DataRecord.put("k2", "v2", 2)).iterator(), 2);

        SSTableReader reader = new SSTableReader(sstFile);
        assertEquals(1, reader.getRefCount());

        CloseableIterator<DataRecord> it = reader.iterator();
        assertEquals(2, reader.getRefCount());

        assertTrue(it.hasNext());
        assertEquals("k1", it.next().getKeyAsString());

        // Abandon early and close explicitly
        it.close();
        assertEquals(1, reader.getRefCount());

        // Repeated close is safe and idempotent
        it.close();
        assertEquals(1, reader.getRefCount());

        assertFalse(it.hasNext());
        reader.close();
    }

    @Test
    @DisplayName("Compaction: compaction failure closes all source iterators and releases references")
    void testCompactionFailureReleasesAllSourceReferences() throws Exception {
        File sstFile1 = tempDir.resolve("src1.sst").toFile();
        File sstFile2 = tempDir.resolve("src2.sst").toFile();

        SSTableWriter w1 = new SSTableWriter(sstFile1, 10, 0, 4096, 0.01);
        w1.write(List.of(DataRecord.put("k1", "v1", 1)).iterator(), 1);
        SSTableWriter w2 = new SSTableWriter(sstFile2, 11, 0, 4096, 0.01);
        w2.write(List.of(DataRecord.put("k2", "v2", 2)).iterator(), 1);

        SSTableReader r1 = new SSTableReader(sstFile1);
        SSTableReader r2 = new SSTableReader(sstFile2);

        assertEquals(1, r1.getRefCount());
        assertEquals(1, r2.getRefCount());

        // Target file in a non-writable / invalid path to simulate compaction I/O failure
        File invalidTarget = new File(tempDir.resolve("non_existent_subdir").toFile(), "compacted.sst");

        assertThrows(IOException.class, () -> {
            CompactionEngine.compact(List.of(r1, r2), invalidTarget, 99, 1, 4096, 0.01, false);
        });

        // Verify that failed compaction did NOT leak iterator references
        assertEquals(1, r1.getRefCount(), "r1 reference was leaked by failed compaction");
        assertEquals(1, r2.getRefCount(), "r2 reference was leaked by failed compaction");

        r1.close();
        r2.close();
    }
}
