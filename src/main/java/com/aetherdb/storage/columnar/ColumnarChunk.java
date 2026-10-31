package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnDefinition;
import com.aetherdb.model.ColumnValue;
import com.aetherdb.model.Schema;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.zip.CRC32;

/**
 * On-disk and in-memory columnar chunk container for high-throughput OLAP analytics.
 * Holds encoded columnar blocks, block-level Min-Max Zone Maps, and table schema.
 * Implements CRC32 checksum verification and atomic installation on write.
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

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = ch.read(buf);
            if (read < 0) {
                throw new EOFException("Premature EOF reading columnar chunk");
            }
        }
    }

    /**
     * Serializes this columnar chunk into an immutable binary file via a temporary file with CRC32.
     */
    public void writeToFile(File file) throws IOException {
        File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
        if (tempFile.exists()) {
            tempFile.delete();
        }

        int estimatedSize = 8192;
        for (String colName : zoneMaps.keySet()) {
            estimatedSize += colName.length() * 2 + 256;
        }
        for (Map.Entry<String, byte[]> entry : encodedColumns.entrySet()) {
            estimatedSize += entry.getKey().length() * 2 + entry.getValue().length + 128;
        }

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        // 1. Header (Magic, Version, ChunkId, RowCount)
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

        // 5. CRC32 Checksum over all payload bytes
        int payloadLen = buffer.position();
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, payloadLen);
        long checksum = crc.getValue();
        buffer.putLong(checksum);

        buffer.flip();

        try {
            try (FileChannel channel = FileChannel.open(tempFile.toPath(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING)) {
                writeFully(channel, buffer);
                channel.force(true);
            }

            try {
                Files.move(tempFile.toPath(), file.toPath(),
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        } finally {
            if (tempFile.exists()) {
                tempFile.delete();
            }
        }
    }

    /**
     * Reads a columnar chunk from an on-disk binary file with full validation and CRC check.
     */
    public static ColumnarChunk readFromFile(File file) throws IOException {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < 26) {
                throw new IOException("Corrupt columnar chunk: file too small (" + fileSize + " bytes) in " + file.getName());
            }

            ByteBuffer buffer = ByteBuffer.allocate((int) fileSize);
            readFully(channel, buffer);
            buffer.flip();

            int magic = buffer.getInt();
            if (magic != Constants.COLUMNAR_MAGIC) {
                throw new IOException("Corrupt columnar chunk magic bytes: 0x" + Integer.toHexString(magic) + " in " + file.getName());
            }
            short version = buffer.getShort();
            if (version != Constants.COLUMNAR_VERSION) {
                throw new IOException("Unsupported columnar chunk version: " + version + " in " + file.getName());
            }

            long chunkId = buffer.getLong();
            int rowCount = buffer.getInt();
            if (rowCount < 0) {
                throw new IOException("Negative row count in columnar chunk " + file.getName());
            }

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
                if (dataLen < 0 || buffer.remaining() < dataLen) {
                    throw new IOException("Corrupt encoded column data length in chunk " + file.getName());
                }
                byte[] data = new byte[dataLen];
                buffer.get(data);
                encodedColumns.put(colName, data);
            }

            if (buffer.remaining() >= 8) {
                int payloadEnd = buffer.position();
                long expectedChecksum = buffer.getLong();
                CRC32 crc = new CRC32();
                crc.update(buffer.array(), 0, payloadEnd);
                if (crc.getValue() != expectedChecksum) {
                    throw new IOException("Columnar chunk CRC mismatch in " + file.getName());
                }
            }

            return new ColumnarChunk(chunkId, schema, rowCount, zoneMaps, encodedColumns);
        }
    }
}
