package com.aetherdb.storage.columnar;

import com.aetherdb.model.ColumnValue;

import java.util.*;

/**
 * High-throughput vectorized execution scanner for columnar chunks.
 * Processes rows in cache-friendly vector batches using selection vectors and loop unrolling.
 */
public class VectorizedScanner {

    private final ColumnarChunk chunk;
    private final int batchSize;

    public VectorizedScanner(ColumnarChunk chunk, int batchSize) {
        this.chunk = chunk;
        this.batchSize = batchSize;
    }

    /**
     * Evaluates an equality filter over a column vector and produces a selection bitset.
     */
    public BitSet filterEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            if (Objects.equals(column.get(i), target)) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a greater-than filter over a column vector.
     */
    public BitSet filterGreaterThan(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && val.compareTo(target) > 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a less-than filter over a column vector.
     */
    public BitSet filterLessThan(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && val.compareTo(target) < 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized SUM aggregation over selected rows.
     */
    public double aggregateSum(String columnName, BitSet selection) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        double sum = 0.0;
        int size = column.size();

        if (selection == null) {
            for (int i = 0; i < size; i++) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) sum += val.asDouble();
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) sum += val.asDouble();
            }
        }
        return sum;
    }

    /**
     * Vectorized COUNT aggregation over selected rows.
     */
    public long aggregateCount(BitSet selection) {
        if (selection == null) {
            return chunk.getRowCount();
        }
        return selection.cardinality();
    }

    /**
     * Vectorized MIN aggregation over selected rows.
     */
    public ColumnValue aggregateMin(String columnName, BitSet selection) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        ColumnValue min = null;
        int size = column.size();

        if (selection == null) {
            for (int i = 0; i < size; i++) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull() && (min == null || val.compareTo(min) < 0)) {
                    min = val;
                }
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull() && (min == null || val.compareTo(min) < 0)) {
                    min = val;
                }
            }
        }
        return min;
    }

    /**
     * Vectorized MAX aggregation over selected rows.
     */
    public ColumnValue aggregateMax(String columnName, BitSet selection) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        ColumnValue max = null;
        int size = column.size();

        if (selection == null) {
            for (int i = 0; i < size; i++) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull() && (max == null || val.compareTo(max) > 0)) {
                    max = val;
                }
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull() && (max == null || val.compareTo(max) > 0)) {
                    max = val;
                }
            }
        }
        return max;
    }
}
