package com.aetherdb.storage.sstable;

import com.aetherdb.common.BinaryUtils;
import com.aetherdb.common.Constants;
import com.aetherdb.model.DataRecord;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Multi-way merge sort compaction engine for AetherDB LSM-Tree.
 * Merges multiple SSTables into a single optimized SSTable:
 * 1. Resolves duplicate keys by preserving only the newest sequence number.
 * 2. Prunes dead tombstone records.
 * 3. Compacts fragmented blocks to reclaim disk space.
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
            // If keys are equal, higher sequence number goes first
            return Long.compare(o.record.getSequenceNumber(), this.record.getSequenceNumber());
        }
    }

    /**
     * Compacts a list of input SSTables into a single target SSTable.
     */
    public static SSTableReader compact(List<SSTableReader> inputTables,
                                        File targetSSTableFile,
                                        long newTableId,
                                        int targetLevel,
                                        int blockSize,
                                        double bloomFpr,
                                        boolean isDeepestLevel) throws IOException {

        PriorityQueue<MergeElement> minHeap = new PriorityQueue<>();

        // Initialize heap with first element from each SSTable iterator
        for (int i = 0; i < inputTables.size(); i++) {
            Iterator<DataRecord> it = inputTables.get(i).iterator();
            if (it.hasNext()) {
                minHeap.add(new MergeElement(it.next(), i, it));
            }
        }

        List<DataRecord> mergedRecords = new ArrayList<>();
        byte[] lastEmittedKey = null;

        while (!minHeap.isEmpty()) {
            MergeElement top = minHeap.poll();
            DataRecord currentRecord = top.record;

            // Advance the iterator of the polled source
            if (top.iterator.hasNext()) {
                minHeap.add(new MergeElement(top.iterator.next(), top.sourceIndex, top.iterator));
            }

            // If we have already emitted a newer version of this key, skip this older version
            if (lastEmittedKey != null && BinaryUtils.compareBytes(lastEmittedKey, currentRecord.getKey()) == 0) {
                continue;
            }

            // Tombstone handling: if it's the deepest level, discard deleted records completely
            if (currentRecord.isTombstone() && isDeepestLevel) {
                lastEmittedKey = currentRecord.getKey();
                continue;
            }

            lastEmittedKey = currentRecord.getKey();
            mergedRecords.add(currentRecord);
        }

        // Write the compacted SSTable
        SSTableWriter writer = new SSTableWriter(targetSSTableFile, newTableId, targetLevel, blockSize, bloomFpr);
        writer.write(mergedRecords.iterator(), mergedRecords.size());

        // Close and clean up input SSTables
        for (SSTableReader r : inputTables) {
            r.close();
            File f = r.getFile();
            if (f.exists()) {
                f.delete();
            }
        }

        return new SSTableReader(targetSSTableFile);
    }
}
