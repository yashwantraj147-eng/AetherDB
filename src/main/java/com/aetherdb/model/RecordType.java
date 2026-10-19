package com.aetherdb.model;

/**
 * Enumeration of operation types supported in WAL and LSM MemTable / SSTable records.
 */
public enum RecordType {
    PUT((byte) 1),
    DELETE((byte) 2); // Tombstone marker

    private final byte code;

    RecordType(byte code) {
        this.code = code;
    }

    public byte getCode() {
        return code;
    }

    public static RecordType fromCode(byte code) {
        return switch (code) {
            case 1 -> PUT;
            case 2 -> DELETE;
            default -> throw new IllegalArgumentException("Unknown RecordType code: " + code);
        };
    }
}
