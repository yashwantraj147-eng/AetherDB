package com.aetherdb.storage.wal;

import java.io.IOException;

/**
 * Exception thrown when a Write-Ahead Log segment has invalid magic numbers,
 * unsupported versions, unrecoverable checksum failures, or corrupted frames.
 */
public class CorruptedWalException extends IOException {

    public CorruptedWalException(String message) {
        super(message);
    }

    public CorruptedWalException(String message, Throwable cause) {
        super(message, cause);
    }
}

