package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Multi-way streaming merge compaction engine for AetherDB LSM-Tree.
 * Merges multiple SSTables into a single target SSTable with:
 * 1. Constant memory footprint using streaming k-way merge.
 * 2. Preservation of highest sequence number for duplicate keys.
 * 3. Safe tombstone pruning only when no older level or pending flush exists.
 * 4. Safe independent SSTable creation without retiring inputs prematurely.
 */
public class CompactionEngine {

    private static class MergeElement implements Comparable<MergeElement> {
        private final DataRecord record;
        private final int sourceIndex;
        private final Iterator<DataRecord> iterator;

        public MergeElement(DataRecord record, int sourceIndex, Iterator<DataRecord> iterator) {
            this.record = record;
            this.sourceIndex = sourceIndex;
            this.iterator = iterator;
        }

        @Override
        public int compareTo(MergeElement o) {
            int cmp = BinaryUtils.compareBytes(this.record.getKey(), o.record.getKey());
            if (cmp != 0) {
                return cmp;
            }
            // Higher sequence number evaluates first
            return Long.compare(o.record.getSequenceNumber(), this.record.getSequenceNumber());
        }
    }

    /**
     * Compacts a list of input SSTables into a single target SSTable using streaming merge.
     * Note: Input SSTables are NOT marked obsolete here. The caller (HybridStorageManager)
     * is responsible for atomic publication and retiring old readers after publication.
     */
    public static SSTableReader compact(List<SSTableReader> inputTables,
                                        File targetSSTableFile,
                                        long newTableId,
                                        int targetLevel,
                                        int blockSize,
                                        double bloomFpr,
                                        boolean isDeepestLevel) throws IOException {

        if (inputTables == null || inputTables.isEmpty()) {
            throw new IllegalArgumentException("Cannot compact empty list of SSTables");
        }

        int totalEstimatedRecords = 0;
        for (SSTableReader r : inputTables) {
            totalEstimatedRecords += (int) Math.min(r.getMetadata().getRecordCount(), Integer.MAX_VALUE / 2);
        }

        Iterator<DataRecord> streamingMergeIterator = new StreamingMergeIterator(inputTables, isDeepestLevel);

        // Write the compacted SSTable using streaming iterator
        SSTableWriter writer = new SSTableWriter(targetSSTableFile, newTableId, targetLevel, blockSize, bloomFpr);
        try {
            writer.write(streamingMergeIterator, totalEstimatedRecords);
            return new SSTableReader(targetSSTableFile);
        } catch (Throwable t) {
            if (targetSSTableFile.exists()) {
                targetSSTableFile.delete();
            }
            throw (t instanceof IOException) ? (IOException) t : new IOException("Compaction failed: " + t.getMessage(), t);
        }
    }

    private static class StreamingMergeIterator implements Iterator<DataRecord> {
        private final PriorityQueue<MergeElement> minHeap = new PriorityQueue<>();
        private final boolean isDeepestLevel;
        private byte[] lastEmittedKey = null;
        private DataRecord nextRecord = null;

        public StreamingMergeIterator(List<SSTableReader> inputTables, boolean isDeepestLevel) {
            this.isDeepestLevel = isDeepestLevel;
            for (int i = 0; i < inputTables.size(); i++) {
                Iterator<DataRecord> it = inputTables.get(i).iterator();
                if (it.hasNext()) {
                    minHeap.add(new MergeElement(it.next(), i, it));
                }
            }
            advance();
        }

        private void advance() {
            nextRecord = null;
            while (!minHeap.isEmpty()) {
                MergeElement top = minHeap.poll();
                DataRecord currentRecord = top.record;

                if (top.iterator.hasNext()) {
                    minHeap.add(new MergeElement(top.iterator.next(), top.sourceIndex, top.iterator));
                }

                // If already emitted a newer version of this key, skip older versions
                if (lastEmittedKey != null && BinaryUtils.compareBytes(lastEmittedKey, currentRecord.getKey()) == 0) {
                    continue;
                }

                // Tombstone handling: if it's the deepest level and no older version can exist, discard deleted records
                if (currentRecord.isTombstone() && isDeepestLevel) {
                    lastEmittedKey = currentRecord.getKey();
                    continue;
                }

                lastEmittedKey = currentRecord.getKey();
                nextRecord = currentRecord;
                return;
            }
        }

        @Override
        public boolean hasNext() {
            return nextRecord != null;
        }

        @Override
        public DataRecord next() {
            if (nextRecord == null) {
                throw new NoSuchElementException();
            }
            DataRecord res = nextRecord;
            advance();
            return res;
        }
    }
}
