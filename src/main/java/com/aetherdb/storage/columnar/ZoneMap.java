package com.aetherdb.storage.columnar;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;

import java.nio.ByteBuffer;

/**
 * Min-Max Zone Map for a column chunk block.
 * Enables aggressive predicate pushdown and block skipping without reading or decompressing data.
 */
public class ZoneMap {

    private final ColumnType type;
    private ColumnValue minVal;
    private ColumnValue maxVal;
    private int nullCount;
    private int rowCount;

    public ZoneMap(ColumnType type) {
        this.type = type;
        this.minVal = null;
        this.maxVal = null;
        this.nullCount = 0;
        this.rowCount = 0;
    }

    public ZoneMap(ColumnType type, ColumnValue minVal, ColumnValue maxVal, int nullCount, int rowCount) {
        this.type = type;
        this.minVal = minVal;
        this.maxVal = maxVal;
        this.nullCount = nullCount;
        this.rowCount = rowCount;
    }

    public void update(ColumnValue val) {
        rowCount++;
        if (val == null || val.isNull()) {
            nullCount++;
            return;
        }

        if (minVal == null || val.compareTo(minVal) < 0) {
            minVal = val;
        }
        if (maxVal == null || val.compareTo(maxVal) > 0) {
            maxVal = val;
        }
    }

    public ColumnValue getMinVal() { return minVal; }
    public ColumnValue getMaxVal() { return maxVal; }
    public int getNullCount() { return nullCount; }
    public int getRowCount() { return rowCount; }

    /**
     * Checks if a target value is guaranteed to NOT be in this chunk based on min/max bounds.
     */
    public boolean canSkipEquals(ColumnValue target) {
        if (minVal == null || maxVal == null) return false;
        return target.compareTo(minVal) < 0 || target.compareTo(maxVal) > 0;
    }

    public boolean canSkipGreaterThan(ColumnValue target) {
        if (maxVal == null) return false;
        // If the maximum value in this chunk is <= target, no value can be > target
        return maxVal.compareTo(target) <= 0;
    }

    public boolean canSkipLessThan(ColumnValue target) {
        if (minVal == null) return false;
        // If the minimum value in this chunk is >= target, no value can be < target
        return minVal.compareTo(target) >= 0;
    }

    /**
     * Serializes ZoneMap to ByteBuffer.
     */
    public void serialize(ByteBuffer buffer) {
        buffer.put(type.getTypeId());
        buffer.putInt(nullCount);
        buffer.putInt(rowCount);

        buffer.put((byte) (minVal != null ? 1 : 0));
        if (minVal != null) {
            serializeValue(buffer, minVal);
        }

        buffer.put((byte) (maxVal != null ? 1 : 0));
        if (maxVal != null) {
            serializeValue(buffer, maxVal);
        }
    }

    private void serializeValue(ByteBuffer buffer, ColumnValue val) {
        switch (type) {
            case INT32 -> buffer.putInt(val.asInt());
            case INT64, TIMESTAMP -> buffer.putLong(val.asLong());
            case FLOAT64 -> buffer.putDouble(val.asDouble());
            case STRING -> BinaryUtils.writeString(buffer, val.asString());
            case BOOLEAN -> buffer.put((byte) (val.asBoolean() ? 1 : 0));
        }
    }

    /**
     * Deserializes ZoneMap from ByteBuffer.
     */
    public static ZoneMap deserialize(ByteBuffer buffer) {
        ColumnType type = ColumnType.fromTypeId(buffer.get());
        int nullCount = buffer.getInt();
        int rowCount = buffer.getInt();

        ColumnValue minVal = null;
        if (buffer.get() == 1) {
            minVal = deserializeValue(buffer, type);
        }

        ColumnValue maxVal = null;
        if (buffer.get() == 1) {
            maxVal = deserializeValue(buffer, type);
        }

        return new ZoneMap(type, minVal, maxVal, nullCount, rowCount);
    }

    private static ColumnValue deserializeValue(ByteBuffer buffer, ColumnType type) {
        return switch (type) {
            case INT32 -> ColumnValue.ofInt(buffer.getInt());
            case INT64 -> ColumnValue.ofLong(buffer.getLong());
            case TIMESTAMP -> ColumnValue.ofTimestamp(buffer.getLong());
            case FLOAT64 -> ColumnValue.ofDouble(buffer.getDouble());
            case STRING -> ColumnValue.ofString(BinaryUtils.readString(buffer));
            case BOOLEAN -> ColumnValue.ofBoolean(buffer.get() == 1);
        };
    }
}
