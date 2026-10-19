package com.aetherdb.storage.engine;

import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.columnar.ColumnarChunk;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Unified storage engine interface for AetherDB.
 * Supports transactional key-value operations and columnar analytical ingestion.
 */
public interface StorageEngine extends AutoCloseable {

    /**
     * Inserts or updates a key-value record.
     */
    void put(byte[] key, byte[] value) throws IOException;

    void put(String key, String value) throws IOException;

    /**
     * Retrieves a key-value record, or null if deleted or not found.
     */
    DataRecord get(byte[] key) throws IOException;

    DataRecord get(String key) throws IOException;

    /**
     * Deletes a key by appending a tombstone record.
     */
    void delete(byte[] key) throws IOException;

    void delete(String key) throws IOException;

    /**
     * Ingests a batch of tabular records into the columnar analytics engine.
     */
    void ingestColumnar(String tableName, List<Map<String, ColumnValue>> records) throws IOException;

    /**
     * Retrieves all on-disk and in-memory columnar chunks for a table.
     */
    List<ColumnarChunk> getColumnarChunks(String tableName);

    /**
     * Manually triggers an in-memory MemTable flush to Level-0 SSTable.
     */
    void flushMemTable() throws IOException;

    /**
     * Manually triggers a compaction pass across SSTables.
     */
    void compact() throws IOException;

    /**
     * Returns a snapshot of internal engine performance metrics.
     */
    Map<String, Object> getMetrics();
}
