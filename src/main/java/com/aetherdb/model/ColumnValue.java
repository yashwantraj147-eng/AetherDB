package com.aetherdb.model;

import com.aetherdb.common.BinaryUtils;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Generic typed value container for columnar and analytics execution.
 */
public class ColumnValue implements Comparable<ColumnValue> {

    private final ColumnType type;
    private final Object value; // null if value is SQL NULL

    public ColumnValue(ColumnType type, Object value) {
        this.type = type;
        this.value = value;
    }

    public static ColumnValue ofInt(int val) { return new ColumnValue(ColumnType.INT32, val); }
    public static ColumnValue ofLong(long val) { return new ColumnValue(ColumnType.INT64, val); }
    public static ColumnValue ofDouble(double val) { return new ColumnValue(ColumnType.FLOAT64, val); }
    public static ColumnValue ofString(String val) { return new ColumnValue(ColumnType.STRING, val); }
    public static ColumnValue ofBoolean(boolean val) { return new ColumnValue(ColumnType.BOOLEAN, val); }
    public static ColumnValue ofTimestamp(long epochMs) { return new ColumnValue(ColumnType.TIMESTAMP, epochMs); }
    public static ColumnValue ofNull(ColumnType type) { return new ColumnValue(type, null); }

    public ColumnType getType() { return type; }
    public Object getValue() { return value; }
    public boolean isNull() { return value == null; }

    public int asInt() {
        if (value instanceof Number n) return n.intValue();
        if (value instanceof String s) return Integer.parseInt(s);
        throw new IllegalStateException("Cannot cast " + value + " to int");
    }

    public long asLong() {
        if (value instanceof Number n) return n.longValue();
        if (value instanceof String s) return Long.parseLong(s);
        throw new IllegalStateException("Cannot cast " + value + " to long");
    }

    public double asDouble() {
        if (value instanceof Number n) return n.doubleValue();
        if (value instanceof String s) return Double.parseDouble(s);
        throw new IllegalStateException("Cannot cast " + value + " to double");
    }

    public String asString() {
        return value == null ? "NULL" : value.toString();
    }

    public boolean asBoolean() {
        if (value instanceof Boolean b) return b;
        if (value instanceof Number n) return n.intValue() != 0;
        if (value instanceof String s) return Boolean.parseBoolean(s);
        throw new IllegalStateException("Cannot cast " + value + " to boolean");
    }

    @Override
    public int compareTo(ColumnValue o) {
        if (this.value == null && o.value == null) return 0;
        if (this.value == null) return -1;
        if (o.value == null) return 1;

        return switch (this.type) {
            case INT32 -> Integer.compare(this.asInt(), o.asInt());
            case INT64, TIMESTAMP -> Long.compare(this.asLong(), o.asLong());
            case FLOAT64 -> Double.compare(this.asDouble(), o.asDouble());
            case STRING -> this.asString().compareTo(o.asString());
            case BOOLEAN -> Boolean.compare(this.asBoolean(), o.asBoolean());
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnValue that = (ColumnValue) o;
        return type == that.type && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, value);
    }

    @Override
    public String toString() {
        return asString();
    }
}
