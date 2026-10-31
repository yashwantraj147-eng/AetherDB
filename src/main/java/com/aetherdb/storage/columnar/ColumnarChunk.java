package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.ColumnType;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.CRC32;

/**
 * Immutable on-disk and in-memory representation of a columnar chunk.
 * Stores compressed columns (RLE/Dictionary) and Zone Maps for fast vector scans.
 * Enforces strict 8-byte CRC32 footer verification and bounded chunk allocation.
 */
public class ColumnarChunk {

    public static final int MAX_CHUNK_FILE_SIZE = 512 * 1024 * 1024; // 512 MB
    public static final int MAX_ROW_COUNT = 10_000_000;

    private final long chunkId;
    private final Schema schema;
    private final int rowCount;
    private final Map<String, ZoneMap> zoneMaps;
    private final Map<String, byte[]> encodedColumns;

    // Cache of decoded columns for vectorized evaluation
    private final ConcurrentHashMap<String, List<ColumnValue>> decodedCache = new ConcurrentHashMap<>();

    public ColumnarChunk(long chunkId, Schema schema, int rowCount,
                         Map<String, ZoneMap> zoneMaps, Map<String, byte[]> encodedColumns) {
        this.chunkId = chunkId;
        this.schema = schema;
        this.rowCount = rowCount;
        this.zoneMaps = Collections.unmodifiableMap(new HashMap<>(zoneMaps));
        this.encodedColumns = Collections.unmodifiableMap(new HashMap<>(encodedColumns));
    }

    public long getChunkId() {
        return chunkId;
    }

    public Schema getSchema() {
        return schema;
    }

    public int getRowCount() {
        return rowCount;
    }

    public ZoneMap getZoneMap(String columnName) {
        return zoneMaps.get(columnName);
    }

    public Map<String, ZoneMap> getZoneMaps() {
        return zoneMaps;
    }

    public byte[] getEncodedColumn(String columnName) {
        return encodedColumns.get(columnName);
    }

    public Map<String, byte[]> getEncodedColumns() {
        return encodedColumns;
    }

    /**
     * Lazily decodes and caches column values in memory.
     */
    public List<ColumnValue> getDecodedColumn(String columnName) {
        return decodedCache.computeIfAbsent(columnName, col -> {
            byte[] encoded = encodedColumns.get(col);
            if (encoded == null) {
                return Collections.emptyList();
            }
            ColumnType type = schema.getColumnType(col);
            if (type == null) {
                type = ColumnType.STRING;
            }
            return ColumnDecoder.decode(encoded, type, rowCount);
        });
    }

    private static void writeFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            ch.write(buf);
        }
    }

    private static void readFully(FileChannel ch, ByteBuffer buf) throws IOException {
        while (buf.hasRemaining()) {
            int read = ch.read(buf);
            if (read == -1) {
                throw new EOFException("Premature EOF while reading columnar chunk file");
            }
        }
    }

    /**
     * Serializes this columnar chunk and writes it to disk via an atomic temporary file.
     */
    public void writeToFile(File file) throws IOException {
        File tempFile = new File(file.getParentFile(), file.getName() + ".tmp");
        if (tempFile.exists()) {
            tempFile.delete();
        }

        // Calculate exact required buffer size
        int totalEncodedBytes = 0;
        for (Map.Entry<String, byte[]> entry : encodedColumns.entrySet()) {
            totalEncodedBytes += 4 + entry.getKey().getBytes(Constants.DEFAULT_CHARSET).length
                    + 5 + entry.getValue().length;
        }

        int totalZoneMapBytes = 0;
        for (Map.Entry<String, ZoneMap> entry : zoneMaps.entrySet()) {
            totalZoneMapBytes += 4 + entry.getKey().getBytes(Constants.DEFAULT_CHARSET).length
                    + 100; // ZoneMap serialized upper bound
        }

        int estimatedSize = 32 + (schema.getColumnDefinitions().size() * 64)
                + totalZoneMapBytes + totalEncodedBytes + 64;

        ByteBuffer buffer = ByteBuffer.allocate(estimatedSize);

        // 1. Header: Magic (4) + Version (2) + ChunkId (8) + RowCount (4)
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
     * Reads a columnar chunk from an on-disk binary file with strict validation and CRC check.
     */
    public static ColumnarChunk readFromFile(File file) throws IOException {
        try (FileChannel channel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            long fileSize = channel.size();
            if (fileSize < 34) {
                throw new IOException("Corrupt columnar chunk: file too small (" + fileSize + " bytes) in " + file.getName());
            }
            if (fileSize > MAX_CHUNK_FILE_SIZE) {
                throw new IOException("Columnar chunk exceeds maximum permitted size (" + fileSize + " > " + MAX_CHUNK_FILE_SIZE + ") in " + file.getName());
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
            if (rowCount < 0 || rowCount > MAX_ROW_COUNT) {
                throw new IOException("Invalid row count " + rowCount + " in columnar chunk " + file.getName());
            }

            Schema schema = Schema.deserialize(buffer);

            int zoneMapCount = BinaryUtils.readVarInt(buffer);
            if (zoneMapCount < 0 || zoneMapCount > 1000) {
                throw new IOException("Invalid zone map count " + zoneMapCount + " in chunk " + file.getName());
            }
            Map<String, ZoneMap> zoneMaps = new HashMap<>();
            for (int i = 0; i < zoneMapCount; i++) {
                String colName = BinaryUtils.readString(buffer);
                ZoneMap zm = ZoneMap.deserialize(buffer);
                zoneMaps.put(colName, zm);
            }

            int colCount = BinaryUtils.readVarInt(buffer);
            if (colCount < 0 || colCount > 1000) {
                throw new IOException("Invalid encoded column count " + colCount + " in chunk " + file.getName());
            }
            Map<String, byte[]> encodedColumns = new HashMap<>();
            for (int i = 0; i < colCount; i++) {
                String colName = BinaryUtils.readString(buffer);
                int dataLen = BinaryUtils.readVarInt(buffer);
                if (dataLen < 0 || buffer.remaining() < dataLen + 8) {
                    throw new IOException("Corrupt encoded column data length " + dataLen + " in chunk " + file.getName());
                }
                byte[] data = new byte[dataLen];
                buffer.get(data);
                encodedColumns.put(colName, data);
            }

            // Strictly require exactly 8 bytes remaining for the CRC32 footer
            if (buffer.remaining() != 8) {
                throw new IOException("Malformed columnar chunk in " + file.getName()
                        + ": expected exactly 8-byte checksum footer, but found " + buffer.remaining() + " bytes remaining");
            }

            int payloadEnd = buffer.position();
            long expectedChecksum = buffer.getLong();
            CRC32 crc = new CRC32();
            crc.update(buffer.array(), 0, payloadEnd);
            long actualChecksum = crc.getValue();
            if (actualChecksum != expectedChecksum) {
                throw new IOException("Columnar chunk CRC mismatch in " + file.getName()
                        + "! Expected=" + expectedChecksum + ", Actual=" + actualChecksum);
            }

            return new ColumnarChunk(chunkId, schema, rowCount, zoneMaps, encodedColumns);
        }
    }
}
