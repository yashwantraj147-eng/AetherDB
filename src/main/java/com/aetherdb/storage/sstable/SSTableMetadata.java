package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;

import java.nio.ByteBuffer;

/**
 * Metadata and footer descriptor for an immutable SSTable.
 * Enforces defensive copying of boundary keys and validates fields on deserialization.
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
        this.minKey = (minKey != null) ? minKey.clone() : null;
        this.maxKey = (maxKey != null) ? maxKey.clone() : null;
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
    public byte[] getMinKey() { return (minKey != null) ? minKey.clone() : null; }
    public byte[] getMaxKey() { return (maxKey != null) ? maxKey.clone() : null; }
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
        if (key == null) return false;
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
     * Deserializes metadata footer from ByteBuffer with integrity checks.
     */
    public static SSTableMetadata deserialize(ByteBuffer buf) throws CorruptedSSTableException {
        if (buf.remaining() < 50) {
            throw new CorruptedSSTableException("SSTable footer too small: " + buf.remaining());
        }

        int magic = buf.getInt();
        if (magic != Constants.SSTABLE_MAGIC) {
            throw new CorruptedSSTableException("Corrupt SSTable magic: 0x" + Integer.toHexString(magic));
        }
        short version = buf.getShort();
        if (version != Constants.SSTABLE_VERSION) {
            throw new CorruptedSSTableException("Unsupported SSTable version: " + version);
        }

        long tableId = buf.getLong();
        int level = buf.getInt();

        int minKeyLen = BinaryUtils.readVarInt(buf);
        if (minKeyLen < 0 || minKeyLen > 65536) {
            throw new CorruptedSSTableException("Invalid SSTable minKey length: " + minKeyLen);
        }
        byte[] minKey = minKeyLen > 0 ? new byte[minKeyLen] : null;
        if (minKey != null) {
            if (buf.remaining() < minKeyLen) throw new CorruptedSSTableException("Buffer underflow reading minKey");
            buf.get(minKey);
        }

        int maxKeyLen = BinaryUtils.readVarInt(buf);
        if (maxKeyLen < 0 || maxKeyLen > 65536) {
            throw new CorruptedSSTableException("Invalid SSTable maxKey length: " + maxKeyLen);
        }
        byte[] maxKey = maxKeyLen > 0 ? new byte[maxKeyLen] : null;
        if (maxKey != null) {
            if (buf.remaining() < maxKeyLen) throw new CorruptedSSTableException("Buffer underflow reading maxKey");
            buf.get(maxKey);
        }

        if (buf.remaining() < 56) {
            throw new CorruptedSSTableException("Buffer underflow reading SSTable metadata numeric fields");
        }

        long minSeq = buf.getLong();
        long maxSeq = buf.getLong();
        long recordCount = buf.getLong();
        long dataSize = buf.getLong();
        long indexOffset = buf.getLong();
        int indexLength = buf.getInt();
        long bloomOffset = buf.getLong();
        int bloomLength = buf.getInt();
        long createdAt = buf.getLong();

        if (recordCount < 0 || dataSize < 0 || indexOffset < 0 || indexLength < 0 || bloomOffset < 0 || bloomLength < 0) {
            throw new CorruptedSSTableException("Negative offset or length in SSTable metadata");
        }

        return new SSTableMetadata(tableId, level, minKey, maxKey, minSeq, maxSeq,
                recordCount, dataSize, indexOffset, indexLength, bloomOffset, bloomLength, createdAt);
    }
}
