package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.Schema;
import com.aetherdb.storage.columnar.ColumnDecoder;
import com.aetherdb.storage.columnar.ColumnEncoder;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.engine.HybridStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class ColumnarStorageTest {

    private Path tempDir;
    private HybridStorageManager storage;

    @BeforeEach
    void setUp() throws Exception {
        tempDir = Files.createTempDirectory("aether_columnar_test_");
        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        config.setProperty("columnar.chunk.max.rows", "5"); // Small chunk threshold for testing
        config.validateAndCreateDirectories();

        storage = new HybridStorageManager(config);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (storage != null) {
            storage.close();
        }
        if (Files.exists(tempDir)) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
    }

    @Test
    @DisplayName("Encoder handles long strings, empty strings, and Unicode without buffer overflow")
    void testColumnEncoderBufferSizingAndUnicode() {
        ColumnEncoder encoder = new ColumnEncoder(ColumnType.STRING, 0.70);
        ColumnDecoder decoder = new ColumnDecoder(ColumnType.STRING);

        String longString = "A".repeat(5000);
        String unicodeString = "Hello 🌍 World! 数据库 HTAP 测试 \uD83D\uDE80";
        String emptyString = "";

        List<ColumnValue> values = List.of(
                ColumnValue.ofString(longString),
                ColumnValue.ofString(unicodeString),
                ColumnValue.ofString(emptyString),
                ColumnValue.ofNull(ColumnType.STRING),
                ColumnValue.ofString("standard_short_string")
        );

        byte[] encoded = assertDoesNotThrow(() -> encoder.encode(values));
        assertNotNull(encoded);
        assertTrue(encoded.length > 5000);

        List<ColumnValue> decoded = decoder.decode(encoded);
        assertEquals(values.size(), decoded.size());
        assertEquals(longString, decoded.get(0).asString());
        assertEquals(unicodeString, decoded.get(1).asString());
        assertEquals(emptyString, decoded.get(2).asString());
        assertTrue(decoded.get(3).isNull());
        assertEquals("standard_short_string", decoded.get(4).asString());
    }

    @Test
    @DisplayName("Security: Table names with path traversal characters are strictly rejected")
    void testPathTraversalRejection() {
        List<Map<String, ColumnValue>> records = List.of(Map.of("col", ColumnValue.ofInt(1)));

        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("../traversal_table", records);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("table/with/slashes", records);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("table\\with\\backslashes", records);
        });
    }

    @Test
    @DisplayName("Schema consistency across batches: unexpected extra columns are rejected")
    void testSchemaExtraColumnsRejected() throws Exception {
        // Batch 1 defines schema with "id" and "amount"
        List<Map<String, ColumnValue>> batch1 = List.of(
                Map.of("id", ColumnValue.ofLong(1), "amount", ColumnValue.ofDouble(10.0))
        );
        storage.ingestColumnar("sales", batch1);

        // Batch 2 with unexpected extra column "extra_field" must be rejected
        List<Map<String, ColumnValue>> batch2 = List.of(
                Map.of("id", ColumnValue.ofLong(2), "amount", ColumnValue.ofDouble(20.0), "extra_field", ColumnValue.ofString("hacked"))
        );
        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("sales", batch2);
        });
    }

    @Test
    @DisplayName("Schema consistency: missing columns are populated with null without error")
    void testSchemaMissingColumnsPopulatedWithNull() throws Exception {
        // Batch 1 has "id" and "val"
        List<Map<String, ColumnValue>> batch1 = List.of(
                Map.of("id", ColumnValue.ofLong(1), "val", ColumnValue.ofString("hello"))
        );
        storage.ingestColumnar("records_test", batch1);

        // Batch 2 missing "val"
        List<Map<String, ColumnValue>> batch2 = List.of(
                Map.of("id", ColumnValue.ofLong(2))
        );
        assertDoesNotThrow(() -> storage.ingestColumnar("records_test", batch2));

        List<ColumnarChunk> chunks = storage.getColumnarChunks("records_test");
        assertEquals(2, chunks.size());
        List<ColumnValue> decodedVal = chunks.get(1).getDecodedColumn("val");
        assertEquals(1, decodedVal.size());
        assertTrue(decodedVal.get(0).isNull());
    }

    @Test
    @DisplayName("Type inference: first row null does not corrupt subsequent numeric types")
    void testFirstRowNullInference() throws Exception {
        Map<String, ColumnValue> row1 = new LinkedHashMap<>();
        row1.put("id", ColumnValue.ofLong(1));
        row1.put("score", ColumnValue.ofNull()); // Untyped null in first row

        Map<String, ColumnValue> row2 = new LinkedHashMap<>();
        row2.put("id", ColumnValue.ofLong(2));
        row2.put("score", ColumnValue.ofDouble(98.5)); // Numeric value in second row

        assertDoesNotThrow(() -> storage.ingestColumnar("exam_scores", List.of(row1, row2)));

        List<ColumnarChunk> chunks = storage.getColumnarChunks("exam_scores");
        assertEquals(1, chunks.size());
        Schema schema = chunks.get(0).getSchema();
        assertEquals(ColumnType.FLOAT64, schema.getColumnType("score"));
    }

    @Test
    @DisplayName("File integrity: 7-byte footer is strictly rejected")
    void testSevenByteFooterRejected() throws Exception {
        storage.ingestColumnar("test_footer", List.of(Map.of("a", ColumnValue.ofLong(10))));
        List<ColumnarChunk> chunks = storage.getColumnarChunks("test_footer");
        assertEquals(1, chunks.size());

        File chunkFile = new File(tempDir.resolve("columnar").toFile(),
                String.format("test_footer_%06d%s", chunks.get(0).getChunkId(), Constants.COLUMNAR_CHUNK_EXT));
        assertTrue(chunkFile.exists());

        // Truncate last byte so remaining bytes at footer is 7 instead of 8
        try (RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw")) {
            raf.setLength(chunkFile.length() - 1);
        }

        assertThrows(IOException.class, () -> {
            ColumnarChunk.readFromFile(chunkFile);
        });
    }

    @Test
    @DisplayName("File integrity: extra trailing bytes after checksum are strictly rejected")
    void testExtraTrailingBytesRejected() throws Exception {
        storage.ingestColumnar("test_extra", List.of(Map.of("a", ColumnValue.ofLong(10))));
        List<ColumnarChunk> chunks = storage.getColumnarChunks("test_extra");
        File chunkFile = new File(tempDir.resolve("columnar").toFile(),
                String.format("test_extra_%06d%s", chunks.get(0).getChunkId(), Constants.COLUMNAR_CHUNK_EXT));

        // Append 4 garbage bytes
        try (RandomAccessFile raf = new RandomAccessFile(chunkFile, "rw")) {
            raf.seek(chunkFile.length());
            raf.writeInt(0xDEADBEEF);
        }

        assertThrows(IOException.class, () -> {
            ColumnarChunk.readFromFile(chunkFile);
        });
    }

    @Test
    @DisplayName("Columnar chunk partitioning: max rows per chunk limit splits large ingestions")
    void testChunkPartitioning() throws Exception {
        List<Map<String, ColumnValue>> records = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            records.add(Map.of("id", ColumnValue.ofLong(i)));
        }

        storage.ingestColumnar("partition_test", records);

        List<ColumnarChunk> chunks = storage.getColumnarChunks("partition_test");
        assertEquals(3, chunks.size());
        assertEquals(5, chunks.get(0).getRowCount());
        assertEquals(5, chunks.get(1).getRowCount());
        assertEquals(2, chunks.get(2).getRowCount());
    }
}
