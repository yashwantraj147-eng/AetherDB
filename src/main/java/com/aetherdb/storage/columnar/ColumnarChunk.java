package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnDefinition;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.Schema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * On-disk and in-memory columnar chunk container for high-throughput OLAP analytics.
 * Holds encoded columnar blocks, block-level Min-Max Zone Maps, and table schema.
 */
public class ColumnarChunk {

    private final long chunkId;
    private final Schema schema;
    private final int rowCount;
    private final Map<String, ZoneMap> zoneMaps;
    private final Map<String, byte[]> encodedColumns;
    private final Map<String, List<ColumnValue>> decodedCache;

    public ColumnarChunk(long chunkId, Schema schema, int rowCount,
                         Map<String, ZoneMap> zoneMaps,
                         Map<String, byte[]> encodedColumns) {
        this.chunkId = chunkId;
        this.schema = schema;
        this.rowCount = rowCount;
        this.zoneMaps = Collections.unmodifiableMap(zoneMaps);
        this.encodedColumns = Collections.unmodifiableMap(encodedColumns);
        this.decodedCache = new HashMap<>();
    }

    public long getChunkId() { return chunkId; }
    public Schema getSchema() { return schema; }
    public int getRowCount() { return rowCount; }

    public ZoneMap getZoneMap(String columnName) {
        return zoneMaps.get(columnName);
    }

    public byte[] getEncodedColumn(String columnName) {
        return encodedColumns.get(columnName);
    }

    /**
     * Lazily decodes and caches a column for vectorized evaluation.
     */
    public synchronized List<ColumnValue> getDecodedColumn(String columnName) {
        List<ColumnValue> cached = decodedCache.get(columnName);
        if (cached != null) return cached;

        byte[] encoded = encodedColumns.get(columnName);
        if (encoded == null) {
            throw new IllegalArgumentException("Column not found in chunk: " + columnName);
        }

        ColumnDefinition def = schema.getColumn(columnName);
        ColumnDecoder decoder = new ColumnDecoder(def.getType());
        List<ColumnValue> decoded = decoder.decode(encoded);
        decodedCache.put(columnName, decoded);
        return decoded;
    }

    /**
     * Serializes this columnar chunk into an immutable binary file.
     */
    public void writeToFile(File file) throws IOException {
        try (FileChannel channel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {

            // Calculate buffer size with ample margin for schema, zone maps, and column names
            int estimatedSize = 8192;
            for (String colName : zoneMaps.keySet()) {
                estimatedSize += colName.length() * 2 + 256;
            }
            for (Map.Entry<String, byte[]> entry : encodedColumns.entrySet()) {
                estimatedSize += entry.getKey().length() * 2 + entry.getValue().length + 128;
            }

            ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

            // 1. Header
            buffer.putInt(Constants.COLUMNAR_MAGIC);
            buffer.putShort(Constants.COLUMNAR_VERSION);
            buffer.putLong(chunkId);
            buffer.putInt(rowCount);

            // 2. Schema
            schema.serialize(buffer);

            // 3. Zone Maps
            BinaryUtils.writeVarInt(buffer, zoneMaps.size());
            for (Map.Entry<String, ZoneMap> entry : zoneMaps.entrySet()) {
                BinaryUtils.writeString(buffer, entry.getKey());
                entry.getValue().serialize(buffer);
            }

            // 4. Encoded Columns
            BinaryUtils.writeVarInt(buffer, encodedColumns.size());
            for (Map.Entry<String, byte[]> entry : encodedColumns.entrySet()) {
                BinaryUtils.writeString(buffer, entry.getKey());
                byte[] colBytes = entry.getValue();
                BinaryUtils.writeVarInt(buffer, colBytes.length);
                buffer.put(colBytes);
            }

            buffer.flip();
            channel.write(buffer);
            channel.force(true);
        }
    }

    /**
     * Reads a columnar chunk from an on-disk binary file.
     */
    public static ColumnarChunk readFromFile(File file) throws IOException {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            ByteBuffer buffer = ByteBuffer.allocate((int) channel.size());
            channel.read(buffer);
            buffer.flip();

            int magic = buffer.getInt();
            if (magic != Constants.COLUMNAR_MAGIC) {
                throw new IllegalStateException("Corrupt columnar chunk magic bytes: 0x" + Integer.toHexString(magic));
            }
            short version = buffer.getShort();
            long chunkId = buffer.getLong();
            int rowCount = buffer.getInt();

            Schema schema = Schema.deserialize(buffer);

            int zoneMapCount = BinaryUtils.readVarInt(buffer);
            Map<String, ZoneMap> zoneMaps = new HashMap<>();
            for (int i = 0; i < zoneMapCount; i++) {
                String colName = BinaryUtils.readString(buffer);
                ZoneMap zm = ZoneMap.deserialize(buffer);
                zoneMaps.put(colName, zm);
            }

            int colCount = BinaryUtils.readVarInt(buffer);
            Map<String, byte[]> encodedColumns = new HashMap<>();
            for (int i = 0; i < colCount; i++) {
                String colName = BinaryUtils.readString(buffer);
                int dataLen = BinaryUtils.readVarInt(buffer);
                byte[] data = new byte[dataLen];
                buffer.get(data);
                encodedColumns.put(colName, data);
            }

            return new ColumnarChunk(chunkId, schema, rowCount, zoneMaps, encodedColumns);
        }
    }
}
