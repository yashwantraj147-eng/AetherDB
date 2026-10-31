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

    /**
     * Immutable snapshot holding MemTable State and its active flush CompletableFuture.
     * Guarantees atomic state transitions and future publication.
     */
    public static final class FlushState {
        private final State state;
        private final CompletableFuture<SSTableReader> future;

        public FlushState(State state, CompletableFuture<SSTableReader> future) {
            this.state = state;
            this.future = future;
        }

        public State getState() {
            return state;
        }

        public CompletableFuture<SSTableReader> getFuture() {
            return future;
        }
    }

    private final long id;
    private final ConcurrentSkipListMap<byte[], DataRecord> map;
    private final AtomicLong byteSize = new AtomicLong(0);
    private final AtomicReference<FlushState> flushState = new AtomicReference<>(new FlushState(State.ACTIVE, null));

    private volatile File associatedWalFile;

    private volatile long minSequenceNumber = Long.MAX_VALUE;
    private volatile long maxSequenceNumber = Long.MIN_VALUE;

    public MemTable(long id) {
        this.id = id;
        this.map = new ConcurrentSkipListMap<>(BinaryUtils::compareBytes);
    }

    public State getState() {
        return flushState.get().getState();
    }

    public boolean transitionTo(State expected, State newState) {
        FlushState cur = flushState.get();
        if (cur.getState() == expected) {
            return flushState.compareAndSet(cur, new FlushState(newState, cur.getFuture()));
        }
        return false;
    }

    public void setState(State newState) {
        flushState.updateAndGet(cur -> new FlushState(newState, cur.getFuture()));
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
        return flushState.get().getFuture();
    }

    public void setFlushFuture(CompletableFuture<SSTableReader> flushFuture) {
        flushState.updateAndGet(cur -> new FlushState(cur.getState(), flushFuture));
    }

    public FlushState getFlushState() {
        return flushState.get();
    }

    /**
     * Atomically claims ownership of a flush attempt for this MemTable.
     * If the table is in SEALED, QUEUED, or FAILED state, it atomically transitions to FLUSHING
     * and publishes a brand-new CompletableFuture representing this attempt in a single CAS.
     * No concurrent thread can observe FLUSHING without the corresponding future available.
     * If another thread already claimed the attempt, returns null so callers can await the active future.
     */
    public synchronized CompletableFuture<SSTableReader> claimFlushAttempt() {
        FlushState current = flushState.get();
        State st = current.getState();
        if (st == State.SEALED || st == State.QUEUED || st == State.FAILED) {
            CompletableFuture<SSTableReader> newFuture = new CompletableFuture<>();
            flushState.set(new FlushState(State.FLUSHING, newFuture));
            return newFuture;
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
