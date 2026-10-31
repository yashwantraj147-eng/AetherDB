package com.aetherdb.model;

import java.io.IOException;

/**
 * Thrown when binary deserialization of a schema fails due to corruption,
 * buffer bounds violation, or malformed data.
 */
public class CorruptedSchemaException extends IOException {

    public CorruptedSchemaException(String message) {
        super(message);
    }

    public CorruptedSchemaException(String message, Throwable cause) {
        super(message, cause);
    }
}
