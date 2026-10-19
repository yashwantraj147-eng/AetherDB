package com.aetherdb.consensus.raft;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.RecordType;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Replicated state machine log entry for Raft consensus.
 */
public class LogPayload {

    private final long term;
    private final long index;
    private final RecordType type;
    private final byte[] key;
    private final byte[] value;
    private final String tableName;

    public LogPayload(long term, long index, RecordType type, byte[] key, byte[] value, String tableName) {
        this.term = term;
        this.index = index;
        this.type = type;
        this.key = key != null ? key : new byte[0];
        this.value = value != null ? value : new byte[0];
        this.tableName = tableName != null ? tableName : "default";
    }

    public long getTerm() { return term; }
    public long getIndex() { return index; }
    public RecordType getType() { return type; }
    public byte[] getKey() { return key; }
    public byte[] getValue() { return value; }
    public String getTableName() { return tableName; }

    public String getKeyAsString() {
        return new String(key, Constants.DEFAULT_CHARSET);
    }

    public String getValueAsString() {
        return new String(value, Constants.DEFAULT_CHARSET);
    }

    /**
     * Serializes this log entry to ByteBuffer.
     */
    public void serialize(ByteBuffer buffer) {
        buffer.putLong(term);
        buffer.putLong(index);
        buffer.put(type.getCode());
        BinaryUtils.writeString(buffer, tableName);

        BinaryUtils.writeVarInt(buffer, key.length);
        buffer.put(key);

        BinaryUtils.writeVarInt(buffer, value.length);
        buffer.put(value);
    }

    /**
     * Deserializes a log entry from ByteBuffer.
     */
    public static LogPayload deserialize(ByteBuffer buffer) {
        long term = buffer.getLong();
        long index = buffer.getLong();
        RecordType type = RecordType.fromCode(buffer.get());
        String tableName = BinaryUtils.readString(buffer);

        int keyLen = BinaryUtils.readVarInt(buffer);
        byte[] key = new byte[keyLen];
        buffer.get(key);

        int valLen = BinaryUtils.readVarInt(buffer);
        byte[] val = new byte[valLen];
        buffer.get(val);

        return new LogPayload(term, index, type, key, val, tableName);
    }
}
