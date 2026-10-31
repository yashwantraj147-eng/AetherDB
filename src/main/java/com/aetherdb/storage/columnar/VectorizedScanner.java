package com.aetherdb.storage.columnar;

import com.aetherdb.model.ColumnValue;

import java.util.*;
import java.util.regex.Pattern;

/**
 * High-throughput vectorized execution scanner for columnar chunks.
 * Evaluates filter predicates using selection bitsets and executes vectorized aggregations
 * with proper null and boundary handling.
 */
public class VectorizedScanner {

    private final ColumnarChunk chunk;
    private final int batchSize;

    public VectorizedScanner(ColumnarChunk chunk, int batchSize) {
        this.chunk = chunk;
        this.batchSize = Math.max(64, batchSize);
    }

    public int getBatchSize() {
        return batchSize;
    }

    public ColumnarChunk getChunk() {
        return chunk;
    }

    /**
     * Evaluates an equality filter over a column vector.
     */
    public BitSet filterEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (Objects.equals(val, target)) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a not-equals filter over a column vector.
     * In standard SQL semantics, NULL values evaluate to UNKNOWN and are excluded.
     */
    public BitSet filterNotEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && !Objects.equals(val, target)) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a strict greater-than filter (> target).
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
     * Evaluates an inclusive greater-than-or-equals filter (>= target).
     */
    public BitSet filterGreaterThanOrEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && val.compareTo(target) >= 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a strict less-than filter (< target).
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
     * Evaluates an inclusive less-than-or-equals filter (<= target).
     */
    public BitSet filterLessThanOrEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        int size = column.size();

        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && val.compareTo(target) <= 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Evaluates a SQL LIKE pattern (supporting % and _ wildcards).
     */
    public BitSet filterLike(String columnName, String likePattern) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        if (likePattern == null) return selection;

        String regex = "^" + Pattern.quote(likePattern)
                .replace("%", "\\E.*\\Q")
                .replace("_", "\\E.\\Q") + "$";
        Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

        int size = column.size();
        for (int i = 0; i < size; i++) {
            ColumnValue val = column.get(i);
            if (val != null && !val.isNull() && val.asString() != null) {
                if (pattern.matcher(val.asString()).matches()) {
                    selection.set(i);
                }
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
     * Vectorized COUNT(*) aggregation over selected rows.
     */
    public long aggregateCount(BitSet selection) {
        if (selection == null) {
            return chunk.getRowCount();
        }
        return selection.cardinality();
    }

    /**
     * Vectorized COUNT(column) aggregation that counts only non-null values.
     */
    public long aggregateCountNonNull(String columnName, BitSet selection) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        long count = 0;
        int size = column.size();

        if (selection == null) {
            for (int i = 0; i < size; i++) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) count++;
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) count++;
            }
        }
        return count;
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
