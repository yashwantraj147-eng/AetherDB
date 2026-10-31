package com.aetherdb.storage.sstable;

import java.io.IOException;

/**
 * Exception thrown when an SSTable file fails integrity checks, has invalid
 * magic numbers, corrupted data blocks, CRC mismatches, or malformed metadata.
 */
public class CorruptedSSTableException extends IOException {

    public CorruptedSSTableException(String message) {
        super(message);
    }

    public CorruptedSSTableException(String message, Throwable cause) {
        super(message, cause);
    }
}
