package com.aetherdb.storage.memtable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Lock-free in-memory buffer for AetherDB LSM-Tree.
 * Uses ConcurrentSkipListMap keyed by raw byte array with custom lexicographical comparator.
 * Tracks approximate memory footprint and sequence number bounds.
 */
public class MemTable implements Iterable<DataRecord> {

    private final long id;
    private final ConcurrentSkipListMap<byte[], DataRecord> map;
    private final AtomicLong byteSize = new AtomicLong(0);

    private volatile long minSequenceNumber = Long.MAX_VALUE;
    private volatile long maxSequenceNumber = Long.MIN_VALUE;

    public MemTable(long id) {
        this.id = id;
        this.map = new ConcurrentSkipListMap<>(BinaryUtils::compareBytes);
    }

    public long getId() {
        return id;
    }

    /**
     * Inserts or updates a record in the MemTable.
     */
    public void put(DataRecord record) {
        DataRecord previous = map.put(record.getKey(), record);

        long delta = record.estimatedSize();
        if (previous != null) {
            delta -= previous.estimatedSize();
        }
        byteSize.addAndGet(delta);

        long seq = record.getSequenceNumber();
        if (seq < minSequenceNumber) minSequenceNumber = seq;
        if (seq > maxSequenceNumber) maxSequenceNumber = seq;
    }

    /**
     * Retrieves the latest version of a record for the given key.
     * Returns null if key is not present.
     */
    public DataRecord get(byte[] key) {
        return map.get(key);
    }

    public boolean containsKey(byte[] key) {
        return map.containsKey(key);
    }

    public long getByteSize() {
        return Math.max(0, byteSize.get());
    }

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public long getMinSequenceNumber() {
        return minSequenceNumber == Long.MAX_VALUE ? 0 : minSequenceNumber;
    }

    public long getMaxSequenceNumber() {
        return maxSequenceNumber == Long.MIN_VALUE ? 0 : maxSequenceNumber;
    }

    public Collection<DataRecord> records() {
        return map.values();
    }

    @Override
    public Iterator<DataRecord> iterator() {
        return map.values().iterator();
    }

    public void clear() {
        map.clear();
        byteSize.set(0);
    }
}
