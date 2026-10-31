package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.columnar.ColumnDecoder;
import com.aetherdb.storage.columnar.ColumnEncoder;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.engine.HybridStorageManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
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
    @DisplayName("Schema validation: subsequent rows with mismatched column types throw error")
    void testSchemaMismatchValidation() {
        List<Map<String, ColumnValue>> records = List.of(
                Map.of("id", ColumnValue.ofLong(1), "price", ColumnValue.ofDouble(19.99)),
                Map.of("id", ColumnValue.ofLong(2), "price", ColumnValue.ofString("INVALID_TYPE_STRING"))
        );

        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("products", records);
        });
    }

    @Test
    @DisplayName("Columnar chunk partitioning: max rows per chunk limit splits large ingestions")
    void testChunkPartitioning() throws Exception {
        // config.getColumnarMaxRows() is set to 5 in setUp()
        List<Map<String, ColumnValue>> records = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            records.add(Map.of("id", ColumnValue.ofLong(i)));
        }

        storage.ingestColumnar("partition_test", records);

        List<ColumnarChunk> chunks = storage.getColumnarChunks("partition_test");
        // 12 records with chunk size 5 -> 3 chunks (5, 5, 2)
        assertEquals(3, chunks.size());
        assertEquals(5, chunks.get(0).getRowCount());
        assertEquals(5, chunks.get(1).getRowCount());
        assertEquals(2, chunks.get(2).getRowCount());
    }
}
