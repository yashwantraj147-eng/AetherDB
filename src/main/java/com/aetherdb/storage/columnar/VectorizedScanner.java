package com.aetherdb.storage.columnar;

import com.aetherdb.model.ColumnType;
import com.aetherdb.model.ColumnValue;

import java.util.BitSet;
import java.util.List;
import java.util.regex.Pattern;

/**
 * High-speed vectorized scanner operating on compressed ColumnarChunks in block batches.
 * Evaluates filter predicates using SIMD-style BitSet masks and computes aggregations
 * with zero intermediate allocations.
 */
public class VectorizedScanner {

    private final ColumnarChunk chunk;
    private final int batchSize;

    public VectorizedScanner(ColumnarChunk chunk, int batchSize) {
        this.chunk = chunk;
        this.batchSize = batchSize;
    }

    public ColumnarChunk getChunk() {
        return chunk;
    }

    public boolean isIntegerColumn(String columnName) {
        ColumnType type = chunk.getSchema().getColumnType(columnName);
        return type == ColumnType.INT32 || type == ColumnType.INT64 || type == ColumnType.TIMESTAMP;
    }

    /**
     * Vectorized filter: col == targetValue
     */
    public BitSet filterEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) == 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized filter: col != targetValue
     */
    public BitSet filterNotEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) != 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized filter: col > targetValue
     */
    public BitSet filterGreaterThan(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) > 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized filter: col >= targetValue
     */
    public BitSet filterGreaterThanOrEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) >= 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized filter: col < targetValue
     */
    public BitSet filterLessThan(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) < 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized filter: col <= targetValue
     */
    public BitSet filterLessThanOrEquals(String columnName, ColumnValue target) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        for (int i = 0; i < column.size(); i++) {
            ColumnValue val = column.get(i);
            if (val != null && val.compareTo(target) <= 0) {
                selection.set(i);
            }
        }
        return selection;
    }

    /**
     * Vectorized LIKE pattern matching (SQL % and _ support).
     */
    public BitSet filterLike(String columnName, String likePattern) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        BitSet selection = new BitSet(column.size());
        if (likePattern == null) return selection;

        String regex = "^" + Pattern.quote(likePattern)
                .replace("%", "\\E.*\\Q")
                .replace("_", "\\E.\\Q") + "$";
        Pattern pattern = Pattern.compile(regex);

        for (int i = 0; i < column.size(); i++) {
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
     * Vectorized SUM aggregation over selected rows (floating point).
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
     * Vectorized SUM aggregation preserving 64-bit integer precision.
     */
    public long aggregateSumLong(String columnName, BitSet selection) {
        List<ColumnValue> column = chunk.getDecodedColumn(columnName);
        long sum = 0;
        int size = column.size();

        if (selection == null) {
            for (int i = 0; i < size; i++) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) sum += val.asLong();
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) sum += val.asLong();
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
     * Vectorized non-null COUNT aggregation over selected rows.
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
                if (val != null && !val.isNull()) {
                    if (min == null || val.compareTo(min) < 0) min = val;
                }
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) {
                    if (min == null || val.compareTo(min) < 0) min = val;
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
                if (val != null && !val.isNull()) {
                    if (max == null || val.compareTo(max) > 0) max = val;
                }
            }
        } else {
            for (int i = selection.nextSetBit(0); i >= 0 && i < size; i = selection.nextSetBit(i + 1)) {
                ColumnValue val = column.get(i);
                if (val != null && !val.isNull()) {
                    if (max == null || val.compareTo(max) > 0) max = val;
                }
            }
        }
        return max;
    }
}
