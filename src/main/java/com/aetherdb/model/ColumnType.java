package com.aetherdb.model;

/**
 * Data types supported by the AetherDB columnar analytics storage engine.
 */
public enum ColumnType {
    INT32((byte) 1, 4),
    INT64((byte) 2, 8),
    FLOAT64((byte) 3, 8),
    STRING((byte) 4, -1),      // Variable length
    BOOLEAN((byte) 5, 1),
    TIMESTAMP((byte) 6, 8);    // Epoch milliseconds

    private final byte typeId;
    private final int fixedSize;

    ColumnType(byte typeId, int fixedSize) {
        this.typeId = typeId;
        this.fixedSize = fixedSize;
    }

    public byte getTypeId() {
        return typeId;
    }

    public int getFixedSize() {
        return fixedSize;
    }

    public boolean isVariableLength() {
        return fixedSize < 0;
    }

    public static ColumnType fromTypeId(byte typeId) {
        for (ColumnType t : values()) {
            if (t.typeId == typeId) return t;
        }
        throw new IllegalArgumentException("Unknown ColumnType ID: " + typeId);
    }
}
