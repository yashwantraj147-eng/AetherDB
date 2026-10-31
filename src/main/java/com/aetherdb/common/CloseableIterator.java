package com.aetherdb.common;

import java.io.Closeable;
import java.util.Iterator;

/**
 * An Iterator that manages underlying resources and must be closed when iteration completes
 * or is abandoned early.
 *
 * @param <T> element type
 */
public interface CloseableIterator<T> extends Iterator<T>, Closeable {

    @Override
    void close();
}
