package com.aetherdb.model;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Core record unit for AetherDB LSM-Tree Key-Value engine.
 * Records are sorted lexicographically by key; ties are broken by sequenceNumber descending
 * so that the most recent record version is evaluated first.
 */
public class DataRecord implements Comparable<DataRecord> {

    private final byte[] key;
    private final byte[] value;
    private final long timestamp;
    private final long sequenceNumber;
    private final RecordType type;

    public DataRecord(byte[] key, byte[] value, long timestamp, long sequenceNumber, RecordType type) {
        this.key = key;
        this.value = (value != null) ? value : Constants.TOMBSTONE_PAYLOAD;
        this.timestamp = timestamp;
        this.sequenceNumber = sequenceNumber;
        this.type = type;
    }

    public static DataRecord put(byte[] key, byte[] value, long seqNum) {
        return new DataRecord(key, value, System.currentTimeMillis(), seqNum, RecordType.PUT);
    }

    public static DataRecord put(String key, String value, long seqNum) {
        return put(key.getBytes(Constants.DEFAULT_CHARSET), value.getBytes(Constants.DEFAULT_CHARSET), seqNum);
    }

    public static DataRecord delete(byte[] key, long seqNum) {
        return new DataRecord(key, Constants.TOMBSTONE_PAYLOAD, System.currentTimeMillis(), seqNum, RecordType.DELETE);
    }

    public static DataRecord delete(String key, long seqNum) {
        return delete(key.getBytes(Constants.DEFAULT_CHARSET), seqNum);
    }

    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public long getTimestamp() { return timestamp; }
    public long getSequenceNumber() { return sequenceNumber; }
    public RecordType getType() { return type; }
    public boolean isTombstone() { return type == RecordType.DELETE; }

    public String getKeyAsString() {
        return new String(key, Constants.DEFAULT_CHARSET);
    }

    public String getValueAsString() {
        return new String(value, Constants.DEFAULT_CHARSET);
    }

    /**
     * Estimated in-memory byte size of this record.
     */
    public int estimatedSize() {
        return key.length + value.length + 8 + 8 + 1 + 32; // rough object overhead
    }

    /**
     * Serializes this record into a binary format:
     * [type (1B)][keyLen (VarInt)][key][valLen (VarInt)][val][timestamp (8B)][seqNum (8B)]
     */
    public void serialize(ByteBuffer buffer) {
        buffer.put(type.getCode());
        BinaryUtils.writeVarInt(buffer, key.length);
        buffer.put(key);
        BinaryUtils.writeVarInt(buffer, value.length);
        buffer.put(value);
        buffer.putLong(timestamp);
        buffer.putLong(sequenceNumber);
    }

    /**
     * Deserializes a record from a ByteBuffer.
     */
    public static DataRecord deserialize(ByteBuffer buffer) {
        byte typeCode = buffer.get();
        RecordType type = RecordType.fromCode(typeCode);
        int keyLen = BinaryUtils.readVarInt(buffer);
        byte[] key = new byte[keyLen];
        buffer.get(key);
        int valLen = BinaryUtils.readVarInt(buffer);
        byte[] value = new byte[valLen];
        buffer.get(value);
        long timestamp = buffer.getLong();
        long seqNum = buffer.getLong();

        return new DataRecord(key, value, timestamp, seqNum, type);
    }

    @Override
    public int compareTo(DataRecord o) {
        int cmp = BinaryUtils.compareBytes(this.key, o.key);
        if (cmp != 0) {
            return cmp;
        }
        // Higher sequence numbers (newer records) sort first
        return Long.compare(o.sequenceNumber, this.sequenceNumber);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataRecord that = (DataRecord) o;
        return sequenceNumber == that.sequenceNumber &&
               Arrays.equals(key, that.key) &&
               type == that.type;
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Long.hashCode(sequenceNumber);
        result = 31 * result + type.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "DataRecord{" +
                "key='" + getKeyAsString() + '\'' +
                ", isTombstone=" + isTombstone() +
                ", seqNum=" + sequenceNumber +
                ", timestamp=" + timestamp +
                '}';
    }
}
