package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;

import java.nio.ByteBuffer;

/**
 * Metadata and footer descriptor for an immutable SSTable.
 */
public class SSTableMetadata {

    private final long tableId;
    private final int level;
    private final byte[] minKey;
    private final byte[] maxKey;
    private final long minSequenceNumber;
    private final long maxSequenceNumber;
    private final long recordCount;
    private final long dataSize;
    private final long indexOffset;
    private final int indexLength;
    private final long bloomOffset;
    private final int bloomLength;
    private final long createdAt;

    public SSTableMetadata(long tableId, int level, byte[] minKey, byte[] maxKey,
                           long minSequenceNumber, long maxSequenceNumber,
                           long recordCount, long dataSize,
                           long indexOffset, int indexLength,
                           long bloomOffset, int bloomLength,
                           long createdAt) {
        this.tableId = tableId;
        this.level = level;
        this.minKey = minKey;
        this.maxKey = maxKey;
        this.minSequenceNumber = minSequenceNumber;
        this.maxSequenceNumber = maxSequenceNumber;
        this.recordCount = recordCount;
        this.dataSize = dataSize;
        this.indexOffset = indexOffset;
        this.indexLength = indexLength;
        this.bloomOffset = bloomOffset;
        this.bloomLength = bloomLength;
        this.createdAt = createdAt;
    }

    public long getTableId() { return tableId; }
    public int getLevel() { return level; }
    public byte[] getMinKey() { return minKey; }
    public byte[] getMaxKey() { return maxKey; }
    public long getMinSequenceNumber() { return minSequenceNumber; }
    public long getMaxSequenceNumber() { return maxSequenceNumber; }
    public long getRecordCount() { return recordCount; }
    public long getDataSize() { return dataSize; }
    public long getIndexOffset() { return indexOffset; }
    public int getIndexLength() { return indexLength; }
    public long getBloomOffset() { return bloomOffset; }
    public int getBloomLength() { return bloomLength; }
    public long getCreatedAt() { return createdAt; }

    public boolean couldContainKey(byte[] key) {
        if (minKey != null && BinaryUtils.compareBytes(key, minKey) < 0) return false;
        if (maxKey != null && BinaryUtils.compareBytes(key, maxKey) > 0) return false;
        return true;
    }

    /**
     * Serializes metadata to a footer ByteBuffer.
     */
    public ByteBuffer serialize() {
        ByteBuffer buf = ByteBuffer.allocate(256 + (minKey != null ? minKey.length : 0) + (maxKey != null ? maxKey.length : 0));
        buf.putInt(Constants.SSTABLE_MAGIC);
        buf.putShort(Constants.SSTABLE_VERSION);
        buf.putLong(tableId);
        buf.putInt(level);

        BinaryUtils.writeVarInt(buf, minKey != null ? minKey.length : 0);
        if (minKey != null) buf.put(minKey);

        BinaryUtils.writeVarInt(buf, maxKey != null ? maxKey.length : 0);
        if (maxKey != null) buf.put(maxKey);

        buf.putLong(minSequenceNumber);
        buf.putLong(maxSequenceNumber);
        buf.putLong(recordCount);
        buf.putLong(dataSize);
        buf.putLong(indexOffset);
        buf.putInt(indexLength);
        buf.putLong(bloomOffset);
        buf.putInt(bloomLength);
        buf.putLong(createdAt);

        buf.flip();
        return buf;
    }

    /**
     * Deserializes metadata footer from ByteBuffer.
     */
    public static SSTableMetadata deserialize(ByteBuffer buf) {
        int magic = buf.getInt();
        if (magic != Constants.SSTABLE_MAGIC) {
            throw new IllegalStateException("Corrupt SSTable magic: " + Integer.toHexString(magic));
        }
        short version = buf.getShort();
        long tableId = buf.getLong();
        int level = buf.getInt();

        int minKeyLen = BinaryUtils.readVarInt(buf);
        byte[] minKey = minKeyLen > 0 ? new byte[minKeyLen] : null;
        if (minKey != null) buf.get(minKey);

        int maxKeyLen = BinaryUtils.readVarInt(buf);
        byte[] maxKey = maxKeyLen > 0 ? new byte[maxKeyLen] : null;
        if (maxKey != null) buf.get(maxKey);

        long minSeq = buf.getLong();
        long maxSeq = buf.getLong();
        long recordCount = buf.getLong();
        long dataSize = buf.getLong();
        long indexOffset = buf.getLong();
        int indexLength = buf.getInt();
        long bloomOffset = buf.getLong();
        int bloomLength = buf.getInt();
        long createdAt = buf.getLong();

        return new SSTableMetadata(tableId, level, minKey, maxKey, minSeq, maxSeq,
                recordCount, dataSize, indexOffset, indexLength, bloomOffset, bloomLength, createdAt);
    }
}
