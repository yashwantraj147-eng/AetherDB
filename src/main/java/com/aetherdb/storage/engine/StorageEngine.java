package com.aetherdb.storage.engine;

import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.DataRecord;
import com.aetherdb.model.Schema;
import com.aetherdb.storage.columnar.ColumnarChunk;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * StorageEngine interface defining operations for both Key-Value (LSM-Tree)
 * and Columnar Analytics workloads, plus lifecycle management.
 */
public interface StorageEngine extends Closeable {

    /**
     * Inserts or updates a key-value record durably.
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

    /**
     * Explicitly registers a table schema for columnar analytics.
     */
    default void registerTableSchema(Schema schema) {
    }

    default void registerTableSchema(String tableName, Schema schema) {
        registerTableSchema(schema);
    }

    /**
     * Retrieves the registered schema for a columnar table, or null if not yet defined.
     */
    default Schema getTableSchema(String tableName) {
        return null;
    }

    /**
     * Returns the current lifecycle state of the storage engine.
     */
    default LifecycleState getLifecycleState() {
        return LifecycleState.OPEN;
    }

    /**
     * Retries flushing any failed or queued MemTables and attempts to recover from FAILED state.
     */
    default void retryFailedFlushes() throws IOException {
    }
}
