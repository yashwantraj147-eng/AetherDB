package com.aetherdb.test;

import com.aetherdb.common.AetherConfig;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnDefinition;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.Schema;
import com.aetherdb.storage.columnar.ColumnDecoder;
import com.aetherdb.storage.columnar.ColumnEncoder;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.columnar.ZoneMap;
import com.aetherdb.storage.engine.HybridStorageManager;
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
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

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

    @Test
    @DisplayName("Schema inference: initial all-null batch without registered schema is strictly rejected")
    void testInitialAllNullBatchWithoutSchemaRejected() {
        Map<String, ColumnValue> nullRow = new HashMap<>();
        nullRow.put("id", ColumnValue.ofLong(1));
        nullRow.put("unknown_field", ColumnValue.ofNull());

        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("null_test_table", List.of(nullRow));
        });
        assertTrue(ex.getMessage().contains("Cannot infer column type") && ex.getMessage().contains("all values in batch are untyped null"));
    }

    @Test
    @DisplayName("Explicit schema registration: resolves initial all-null batches and accepts subsequent typed batches")
    void testExplicitSchemaRegistrationWithAllNullBatch() throws Exception {
        Schema schema = new Schema("events", List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("latency", ColumnType.FLOAT64)
        ));
        storage.registerTableSchema("events", schema);
        assertEquals(schema, storage.getTableSchema("events"));

        // Ingest initial batch where latency is null
        Map<String, ColumnValue> row1 = new HashMap<>();
        row1.put("id", ColumnValue.ofLong(101));
        row1.put("latency", ColumnValue.ofNull(ColumnType.FLOAT64));
        assertDoesNotThrow(() -> storage.ingestColumnar("events", List.of(row1)));

        // Ingest subsequent batch with valid numeric latency
        Map<String, ColumnValue> row2 = new HashMap<>();
        row2.put("id", ColumnValue.ofLong(102));
        row2.put("latency", ColumnValue.ofDouble(42.5));
        assertDoesNotThrow(() -> storage.ingestColumnar("events", List.of(row2)));

        List<ColumnarChunk> chunks = storage.getColumnarChunks("events");
        assertEquals(2, chunks.size());
        assertEquals(ColumnType.FLOAT64, chunks.get(0).getSchema().getColumnType("latency"));
    }

    @Test
    @DisplayName("Immutability: ColumnarChunk internal state cannot be mutated by caller")
    void testColumnarChunkImmutability() {
        Schema schema = new Schema("test_table", List.of(
                new ColumnDefinition("val", ColumnType.INT64)
        ));
        byte[] originalBytes = new byte[]{1, 2, 3, 4};
        Map<String, byte[]> encodedMap = new HashMap<>();
        encodedMap.put("val", originalBytes);

        Map<String, ZoneMap> zoneMaps = Map.of(
                "val", new ZoneMap(ColumnType.INT64)
        );

        ColumnarChunk chunk = new ColumnarChunk(1L, schema, 1, zoneMaps, encodedMap);

        // Mutating the input array after chunk creation must not affect the chunk
        originalBytes[0] = 99;
        assertEquals(1, chunk.getEncodedColumn("val")[0]);

        // Mutating array returned by getEncodedColumn() must not affect chunk
        byte[] retrieved = chunk.getEncodedColumn("val");
        retrieved[0] = 77;
        assertEquals(1, chunk.getEncodedColumn("val")[0]);

        // Modifying decoded column list returned by getDecodedColumn must throw UnsupportedOperationException
        ColumnEncoder encoder = new ColumnEncoder(ColumnType.INT64, 0.70);
        byte[] validEncoded = encoder.encode(List.of(ColumnValue.ofLong(42)));
        ColumnarChunk validChunk = new ColumnarChunk(2L, schema, 1, zoneMaps, Map.of("val", validEncoded));
        List<ColumnValue> decoded = validChunk.getDecodedColumn("val");
        assertThrows(UnsupportedOperationException.class, () -> {
            decoded.add(ColumnValue.ofLong(999));
        });
    }

    @Test
    @DisplayName("Schema consistency across restart: schema is retained and enforced after reopening database")
    void testSchemaPreservedAcrossRestart() throws Exception {
        Schema schema = new Schema("persistent_table", List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("score", ColumnType.FLOAT64)
        ));
        storage.registerTableSchema("persistent_table", schema);
        storage.ingestColumnar("persistent_table", List.of(
                Map.of("id", ColumnValue.ofLong(1), "score", ColumnValue.ofDouble(95.0))
        ));

        // Reopen storage engine
        storage.close();
        AetherConfig config = new AetherConfig();
        config.setProperty("storage.wal.dir", tempDir.resolve("wal").toString());
        config.setProperty("storage.sstable.dir", tempDir.resolve("sstable").toString());
        config.setProperty("storage.columnar.dir", tempDir.resolve("columnar").toString());
        config.setProperty("storage.data.dir", tempDir.resolve("storage").toString());
        config.validateAndCreateDirectories();
        storage = new HybridStorageManager(config);

        // Verify chunks loaded and schema is preserved
        List<ColumnarChunk> chunks = storage.getColumnarChunks("persistent_table");
        assertEquals(1, chunks.size());
        assertEquals(ColumnType.FLOAT64, chunks.get(0).getSchema().getColumnType("score"));

        // Subsequent valid batch succeeds
        assertDoesNotThrow(() -> {
            storage.ingestColumnar("persistent_table", List.of(
                    Map.of("id", ColumnValue.ofLong(2), "score", ColumnValue.ofDouble(88.0))
            ));
        });

        // Batch with wrong column type fails
        assertThrows(IllegalArgumentException.class, () -> {
            storage.ingestColumnar("persistent_table", List.of(
                    Map.of("id", ColumnValue.ofLong(3), "score", ColumnValue.ofString("invalid_string_score"))
            ));
        });
    }

    @Test
    @DisplayName("Schema registration validation: rejects invalid names, mismatches, and incompatible replacements")
    void testSchemaRegistrationValidation() throws Exception {
        Schema validSchema = new Schema("users", List.of(
                new ColumnDefinition("user_id", ColumnType.INT64),
                new ColumnDefinition("name", ColumnType.STRING)
        ));

        // 1. Mismatched table name between argument and schema
        assertThrows(IllegalArgumentException.class, () -> {
            storage.registerTableSchema("different_table_name", validSchema);
        });

        // 2. Invalid table name (path traversal)
        assertThrows(IllegalArgumentException.class, () -> {
            storage.registerTableSchema("../traversal", new Schema("../traversal", List.of(new ColumnDefinition("c", ColumnType.INT32))));
        });

        // 3. Empty schema definition
        assertThrows(IllegalArgumentException.class, () -> {
            storage.registerTableSchema("empty_schema", new Schema("empty_schema", Collections.emptyList()));
        });

        // 4. Valid registration succeeds
        storage.registerTableSchema("users", validSchema);
        assertEquals(validSchema, storage.getTableSchema("users"));

        // 5. Idempotent re-registration of identical schema succeeds
        assertDoesNotThrow(() -> {
            storage.registerTableSchema("users", validSchema);
        });

        // 6. Incompatible schema replacement throws IllegalArgumentException
        Schema incompatibleSchema = new Schema("users", List.of(
                new ColumnDefinition("user_id", ColumnType.STRING), // Changed type!
                new ColumnDefinition("name", ColumnType.STRING)
        ));
        assertThrows(IllegalArgumentException.class, () -> {
            storage.registerTableSchema("users", incompatibleSchema);
        });
    }

    @Test
    @DisplayName("Schema registration: incompatible schema registration rejected if chunks already exist")
    void testSchemaRegistrationIncompatibleWithExistingChunksRejected() throws Exception {
        storage.ingestColumnar("metrics_tbl", List.of(
                Map.of("id", ColumnValue.ofLong(1), "metric_val", ColumnValue.ofDouble(12.34))
        ));

        Schema incompatible = new Schema("metrics_tbl", List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("metric_val", ColumnType.INT32) // Incompatible type with existing chunk
        ));

        assertThrows(IllegalArgumentException.class, () -> {
            storage.registerTableSchema("metrics_tbl", incompatible);
        });
    }

    @Test
    @DisplayName("ZoneMap Immutability: ZoneMaps in ColumnarChunk are frozen and cannot be mutated")
    void testZoneMapImmutabilityInChunk() throws Exception {
        storage.ingestColumnar("telemetry", List.of(
                Map.of("device_id", ColumnValue.ofLong(10), "temperature", ColumnValue.ofDouble(25.0)),
                Map.of("device_id", ColumnValue.ofLong(20), "temperature", ColumnValue.ofDouble(35.0))
        ));

        List<ColumnarChunk> chunks = storage.getColumnarChunks("telemetry");
        assertEquals(1, chunks.size());
        ColumnarChunk chunk = chunks.get(0);

        ZoneMap tempZm = chunk.getZoneMap("temperature");
        assertNotNull(tempZm);
        assertTrue(tempZm.isFrozen(), "ZoneMap in published chunk must be frozen");
        assertEquals(25.0, tempZm.getMinVal().asDouble(), 0.001);
        assertEquals(35.0, tempZm.getMaxVal().asDouble(), 0.001);

        // Attempting to update a frozen ZoneMap must throw UnsupportedOperationException
        assertThrows(UnsupportedOperationException.class, () -> {
            tempZm.update(ColumnValue.ofDouble(999.0));
        });

        // Statistics remain unchanged
        assertEquals(25.0, tempZm.getMinVal().asDouble(), 0.001);
        assertEquals(35.0, tempZm.getMaxVal().asDouble(), 0.001);
        assertEquals(2, tempZm.getRowCount());
        assertEquals(0, tempZm.getNullCount());

        // Predicate pushdown decisions remain completely accurate
        assertTrue(tempZm.canSkipGreaterThan(ColumnValue.ofDouble(40.0)));
        assertFalse(tempZm.canSkipGreaterThan(ColumnValue.ofDouble(30.0)));
        assertTrue(tempZm.canSkipLessThan(ColumnValue.ofDouble(20.0)));
        assertFalse(tempZm.canSkipLessThan(ColumnValue.ofDouble(26.0)));
    }

    @Test
    @DisplayName("ZoneMap Deserialization: validates counts and min/max consistency")
    void testZoneMapDeserializationValidation() {
        // 1. Negative null count
        ByteBuffer badNullCount = ByteBuffer.allocate(64);
        badNullCount.put(ColumnType.INT32.getTypeId());
        badNullCount.putInt(-1); // invalid negative nullCount
        badNullCount.putInt(10); // rowCount
        badNullCount.put((byte) 0);
        badNullCount.put((byte) 0);
        badNullCount.flip();

        assertThrows(IllegalArgumentException.class, () -> ZoneMap.deserialize(badNullCount));

        // 2. nullCount > rowCount
        ByteBuffer badRatio = ByteBuffer.allocate(64);
        badRatio.put(ColumnType.INT32.getTypeId());
        badRatio.putInt(15); // nullCount > rowCount
        badRatio.putInt(10); // rowCount
        badRatio.put((byte) 0);
        badRatio.put((byte) 0);
        badRatio.flip();

        assertThrows(IllegalArgumentException.class, () -> ZoneMap.deserialize(badRatio));

        // 3. minVal > maxVal
        ByteBuffer badMinMax = ByteBuffer.allocate(64);
        badMinMax.put(ColumnType.INT64.getTypeId());
        badMinMax.putInt(0);
        badMinMax.putInt(10);
        badMinMax.put((byte) 1); // minVal present
        badMinMax.putLong(100L); // min = 100
        badMinMax.put((byte) 1); // maxVal present
        badMinMax.putLong(50L);  // max = 50 (min > max!)
        badMinMax.flip();

        assertThrows(IllegalArgumentException.class, () -> ZoneMap.deserialize(badMinMax));

        // 4. Valid deserialization produces frozen ZoneMap
        ByteBuffer validBuf = ByteBuffer.allocate(64);
        validBuf.put(ColumnType.INT64.getTypeId());
        validBuf.putInt(1);
        validBuf.putInt(10);
        validBuf.put((byte) 1);
        validBuf.putLong(50L);
        validBuf.put((byte) 1);
        validBuf.putLong(100L);
        validBuf.flip();

        ZoneMap zm = ZoneMap.deserialize(validBuf);
        assertNotNull(zm);
        assertTrue(zm.isFrozen());
        assertEquals(50L, zm.getMinVal().asLong());
        assertEquals(100L, zm.getMaxVal().asLong());
        assertThrows(UnsupportedOperationException.class, () -> zm.update(ColumnValue.ofLong(200L)));
    }

    @Test
    @DisplayName("Concurrent schema registration: exactly one schema retained when threads race with incompatible definitions")
    void testConcurrentIncompatibleSchemaRegistration() throws Exception {
        String tableName = "race_table";
        int threadCount = 10;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CyclicBarrier barrier = new CyclicBarrier(threadCount);

        List<ColumnDefinition> colsTypeA = List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("metric", ColumnType.STRING)
        );
        Schema schemaA = new Schema(tableName, colsTypeA);

        List<ColumnDefinition> colsTypeB = List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("metric", ColumnType.FLOAT64)
        );
        Schema schemaB = new Schema(tableName, colsTypeB);

        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger rejectedCount = new AtomicInteger(0);
        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < threadCount; i++) {
            final boolean useA = (i % 2 == 0);
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    storage.registerTableSchema(tableName, useA ? schemaA : schemaB);
                    successCount.incrementAndGet();
                } catch (IllegalArgumentException e) {
                    rejectedCount.incrementAndGet();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        executor.shutdown();

        // Exactly one schema type won
        Schema registered = storage.getTableSchema(tableName);
        assertNotNull(registered);
        boolean isA = registered.equals(schemaA);
        boolean isB = registered.equals(schemaB);
        assertTrue(isA ^ isB, "Retained schema must be either schemaA or schemaB");

        // The threads that submitted the winning schema succeeded (idempotent re-registration);
        // all threads that submitted the conflicting schema were rejected with IllegalArgumentException
        assertEquals(threadCount / 2, successCount.get(), "All threads submitting the winning schema succeed");
        assertEquals(threadCount / 2, rejectedCount.get(), "All threads submitting conflicting schema must be rejected");
    }

    @Test
    @DisplayName("Synchronized schema registration & ingestion: registerTableSchema and ingestColumnar race safely without inconsistent schemas")
    void testSchemaRegistrationSynchronizedWithIngestion() throws Exception {
        String tableName = "sync_ingest_tbl";
        int threads = 6;
        ExecutorService executor = Executors.newFixedThreadPool(threads);
        CyclicBarrier barrier = new CyclicBarrier(threads);

        Schema declaredSchema = new Schema(tableName, List.of(
                new ColumnDefinition("id", ColumnType.INT64),
                new ColumnDefinition("tag", ColumnType.STRING)
        ));

        List<Future<?>> futures = new ArrayList<>();
        AtomicInteger registrationSuccess = new AtomicInteger(0);
        AtomicInteger ingestionSuccess = new AtomicInteger(0);

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                    if (idx % 2 == 0) {
                        storage.registerTableSchema(tableName, declaredSchema);
                        registrationSuccess.incrementAndGet();
                    } else {
                        storage.ingestColumnar(tableName, List.of(
                                Map.of("id", ColumnValue.ofLong(idx), "tag", ColumnValue.ofString("val_" + idx))
                        ));
                        ingestionSuccess.incrementAndGet();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        for (Future<?> f : futures) {
            f.get(5, TimeUnit.SECONDS);
        }
        executor.shutdown();

        assertEquals(declaredSchema, storage.getTableSchema(tableName));
        List<ColumnarChunk> chunks = storage.getColumnarChunks(tableName);
        assertFalse(chunks.isEmpty());
        for (ColumnarChunk c : chunks) {
            assertEquals(declaredSchema, c.getSchema());
        }
    }
}
