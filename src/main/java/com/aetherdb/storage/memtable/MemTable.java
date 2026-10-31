package com.aetherdb.storage.memtable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;
import com.aetherdb.storage.sstable.SSTableReader;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Concurrent in-memory buffer for AetherDB LSM-Tree.
 * Uses ConcurrentSkipListMap keyed by raw byte array with custom lexicographical comparator.
 * Enforces sequence-number monotonicity: an older sequence number will never overwrite
 * a newer sequence number for the same key.
 * Tracks explicit lifecycle states and synchronized attempt futures to prevent duplicate flush executions.
 */
public class MemTable implements Iterable<DataRecord> {

    public enum State {
        ACTIVE,
        SEALED,
        QUEUED,
        FLUSHING,
        INSTALLED,
        FAILED
    }

    private final long id;
    private final ConcurrentSkipListMap<byte[], DataRecord> map;
    private final AtomicLong byteSize = new AtomicLong(0);
    private final AtomicReference<State> state = new AtomicReference<>(State.ACTIVE);

    private volatile File associatedWalFile;
    private volatile CompletableFuture<SSTableReader> flushFuture;

    private volatile long minSequenceNumber = Long.MAX_VALUE;
    private volatile long maxSequenceNumber = Long.MIN_VALUE;

    public MemTable(long id) {
        this.id = id;
        this.map = new ConcurrentSkipListMap<>(BinaryUtils::compareBytes);
    }

    public State getState() {
        return state.get();
    }

    public boolean transitionTo(State expected, State newState) {
        return state.compareAndSet(expected, newState);
    }

    public void setState(State newState) {
        state.set(newState);
    }

    public long getId() {
        return id;
    }

    public File getAssociatedWalFile() {
        return associatedWalFile;
    }

    public void setAssociatedWalFile(File associatedWalFile) {
        this.associatedWalFile = associatedWalFile;
    }

    public CompletableFuture<SSTableReader> getFlushFuture() {
        return flushFuture;
    }

    public void setFlushFuture(CompletableFuture<SSTableReader> flushFuture) {
        this.flushFuture = flushFuture;
    }

    /**
     * Atomically claims ownership of a flush attempt for this MemTable.
     * If the table is in SEALED, QUEUED, or FAILED state, it transitions to FLUSHING,
     * assigns a brand-new CompletableFuture representing this attempt, and returns the future.
     * If another thread already claimed the attempt and it is actively running, returns null
     * so callers can await the active getFlushFuture().
     */
    public synchronized CompletableFuture<SSTableReader> claimFlushAttempt() {
        State current = state.get();
        if (current == State.SEALED || current == State.QUEUED || current == State.FAILED) {
            state.set(State.FLUSHING);
            this.flushFuture = new CompletableFuture<>();
            return this.flushFuture;
        }
        return null;
    }

    /**
     * Inserts or updates a record in the MemTable.
     * Guarantees that a write with an older sequence number never overwrites a newer record.
     */
    public void put(DataRecord record) {
        if (record == null) return;
        byte[] key = record.getKey();

        map.compute(key, (k, existing) -> {
            if (existing == null) {
                byteSize.addAndGet(record.estimatedSize());
                updateSequenceBounds(record.getSequenceNumber());
                return record;
            }
            if (record.getSequenceNumber() >= existing.getSequenceNumber()) {
                long delta = record.estimatedSize() - existing.estimatedSize();
                byteSize.addAndGet(delta);
                updateSequenceBounds(record.getSequenceNumber());
                return record;
            }
            // Retain the newer version
            return existing;
        });
    }

    private synchronized void updateSequenceBounds(long seq) {
        if (seq < minSequenceNumber) minSequenceNumber = seq;
        if (seq > maxSequenceNumber) maxSequenceNumber = seq;
    }

    public DataRecord get(byte[] key) {
        return map.get(key);
    }

    public boolean containsKey(byte[] key) {
        return map.containsKey(key);
    }

    public int getRecordCount() {
        return map.size();
    }

    public int size() {
        return map.size();
    }

    public long getByteSize() {
        return byteSize.get();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public Collection<DataRecord> values() {
        return map.values();
    }

    public long getMinSequenceNumber() {
        return minSequenceNumber;
    }

    public long getMaxSequenceNumber() {
        return maxSequenceNumber;
    }

    @Override
    public Iterator<DataRecord> iterator() {
        return map.values().iterator();
    }
}
