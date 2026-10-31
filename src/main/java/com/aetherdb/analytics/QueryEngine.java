package com.aetherdb.analytics;

import com.aetherdb.model.*;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.columnar.VectorizedScanner;
import com.aetherdb.storage.columnar.ZoneMap;
import com.aetherdb.storage.engine.StorageEngine;

import java.util.*;

/**
 * High-performance Vectorized SQL-like Query Execution Engine.
 * Features:
 * 1. SIMD-style BitSet predicate evaluation with ZoneMap chunk skipping.
 * 2. Vectorized aggregation accumulators preserving 64-bit integer precision.
 * 3. SQL-standard semantics for empty tables and filters (COUNT(*) returns 0, SUM/AVG return null).
 * 4. Strict GROUP BY validation rejecting invalid non-aggregate projections.
 */
public class QueryEngine {

    private final StorageEngine storageEngine;
    private final int batchSize;

    public QueryEngine(StorageEngine storageEngine, int batchSize) {
        this.storageEngine = storageEngine;
        this.batchSize = batchSize;
    }

    public QueryResult execute(QueryRequest request) {
        long startNanos = System.nanoTime();
        if (request == null) {
            throw new IllegalArgumentException("QueryRequest cannot be null");
        }

        List<ColumnarChunk> chunks = storageEngine.getColumnarChunks(request.getTableName());
        validateQuery(request, chunks);

        boolean hasGroupBy = request.getGroupByColumns() != null && !request.getGroupByColumns().isEmpty();
        boolean isAggregateQuery = hasGroupBy;

        if (!isAggregateQuery) {
            for (String expr : request.getSelectExpressions()) {
                String upper = expr.toUpperCase().trim();
                if (isAggregateExpression(upper)) {
                    isAggregateQuery = true;
                    break;
                }
            }
        }

        if (chunks.isEmpty()) {
            List<String> outputCols = new ArrayList<>();
            if (hasGroupBy) {
                outputCols.addAll(request.getGroupByColumns());
                outputCols.addAll(request.getSelectExpressions());
                return new QueryResult(outputCols, Collections.emptyList(), 0.0, 0, 0, 0);
            } else if (isAggregateQuery) {
                outputCols.addAll(request.getSelectExpressions());
                List<Object> row = new ArrayList<>();
                for (String expr : request.getSelectExpressions()) {
                    AggregateAccumulator acc = createAccumulator(expr);
                    row.add(acc.getResult());
                }
                return new QueryResult(outputCols, List.of(row), 0.0, 0, 0, 0);
            } else {
                outputCols.addAll(request.getSelectExpressions());
                return new QueryResult(outputCols, Collections.emptyList(), 0.0, 0, 0, 0);
            }
        }

        if (isAggregateQuery) {
            return executeAggregateQuery(request, chunks, startNanos);
        } else {
            return executeProjectQuery(request, chunks, startNanos);
        }
    }

    private void validateQuery(QueryRequest request, List<ColumnarChunk> chunks) {
        if (request.getTableName() == null || request.getTableName().trim().isEmpty()) {
            throw new IllegalArgumentException("Table name cannot be empty");
        }
        if (request.getLimit() <= 0) {
            throw new IllegalArgumentException("Query limit must be positive, got: " + request.getLimit());
        }
        if (request.getSelectExpressions() == null || request.getSelectExpressions().isEmpty()) {
            throw new IllegalArgumentException("SELECT expressions cannot be empty");
        }

        List<String> groupByCols = request.getGroupByColumns() != null ? request.getGroupByColumns() : Collections.emptyList();
        Set<String> groupBySet = new HashSet<>(groupByCols);

        // Validate each select expression
        for (String expr : request.getSelectExpressions()) {
            String upper = expr.toUpperCase().trim();
            boolean isAgg = isAggregateExpression(upper);
            if (!isAgg) {
                if (!groupByCols.isEmpty() && !groupBySet.contains(expr)) {
                    throw new IllegalArgumentException("Expression '" + expr + "' must appear in the GROUP BY clause or be used in an aggregate function");
                }
            } else {
                if (!isValidAggregateSyntax(upper)) {
                    throw new IllegalArgumentException("Malformed or unsupported aggregate expression: " + expr);
                }
            }
        }

        // Validate columns against schema if chunks exist
        if (!chunks.isEmpty()) {
            Schema schema = chunks.get(0).getSchema();
            Set<String> schemaCols = new HashSet<>();
            for (ColumnDefinition cd : schema.getColumnDefinitions()) {
                schemaCols.add(cd.getName());
            }

            for (String gCol : groupByCols) {
                if (!schemaCols.contains(gCol)) {
                    throw new IllegalArgumentException("Unknown column in GROUP BY: '" + gCol + "'");
                }
            }

            for (String expr : request.getSelectExpressions()) {
                if (isAggregateExpression(expr.toUpperCase().trim())) {
                    String col = extractColumnName(expr);
                    if (!"*".equals(col) && !col.isEmpty() && !schemaCols.contains(col)) {
                        throw new IllegalArgumentException("Unknown column in aggregate expression '" + expr + "': '" + col + "'");
                    }
                } else {
                    if (!schemaCols.contains(expr)) {
                        throw new IllegalArgumentException("Unknown column in SELECT expression: '" + expr + "'");
                    }
                }
            }

            if (request.getPredicates() != null) {
                for (Predicate pred : request.getPredicates()) {
                    if (!schemaCols.contains(pred.getColumnName())) {
                        throw new IllegalArgumentException("Unknown column in predicate: '" + pred.getColumnName() + "'");
                    }
                }
            }
        }
    }

    private static boolean isAggregateExpression(String upper) {
        return upper.startsWith("COUNT(") || upper.startsWith("SUM(") || upper.startsWith("AVG(")
                || upper.startsWith("MIN(") || upper.startsWith("MAX(");
    }

    private static boolean isValidAggregateSyntax(String upper) {
        return (upper.startsWith("COUNT(") || upper.startsWith("SUM(") || upper.startsWith("AVG(")
                || upper.startsWith("MIN(") || upper.startsWith("MAX(")) && upper.endsWith(")");
    }

    private interface AggregateAccumulator {
        void accumulate(ColumnValue val);
        Object getResult();
    }

    private static class CountStarAccumulator implements AggregateAccumulator {
        private long count = 0;
        @Override
        public void accumulate(ColumnValue val) { count++; }
        @Override
        public Object getResult() { return count; }
    }

    private static class CountColumnAccumulator implements AggregateAccumulator {
        private long count = 0;
        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) count++;
        }
        @Override
        public Object getResult() { return count; }
    }

    private static class SumAccumulator implements AggregateAccumulator {
        private long integerSum = 0;
        private double doubleSum = 0.0;
        private boolean isIntegerOnly = true;
        private boolean hasValue = false;

        public void addLong(long val) {
            hasValue = true;
            if (isIntegerOnly) {
                try {
                    integerSum = Math.addExact(integerSum, val);
                } catch (ArithmeticException overflow) {
                    isIntegerOnly = false;
                    doubleSum = (double) integerSum + (double) val;
                }
            } else {
                doubleSum += val;
            }
        }

        public void addDouble(double val) {
            hasValue = true;
            if (isIntegerOnly) {
                doubleSum = (double) integerSum;
                isIntegerOnly = false;
            }
            doubleSum += val;
        }

        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) {
                if (isIntegerOnly && (val.getType() == ColumnType.INT32 || val.getType() == ColumnType.INT64 || val.getType() == ColumnType.TIMESTAMP)) {
                    addLong(val.asLong());
                } else {
                    addDouble(val.asDouble());
                }
            }
        }

        @Override
        public Object getResult() {
            if (!hasValue) return null;
            if (isIntegerOnly) {
                return Long.valueOf(integerSum);
            }
            return Double.valueOf(doubleSum);
        }
    }

    private static class AvgAccumulator implements AggregateAccumulator {
        private double sum = 0.0;
        private long count = 0;
        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) {
                sum += val.asDouble();
                count++;
            }
        }
        @Override
        public Object getResult() { return count > 0 ? (sum / count) : null; }
    }

    private static class MinAccumulator implements AggregateAccumulator {
        private ColumnValue min = null;
        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) {
                if (min == null || val.compareTo(min) < 0) {
                    min = val;
                }
            }
        }
        @Override
        public Object getResult() { return min != null ? min.getValue() : null; }
    }

    private static class MaxAccumulator implements AggregateAccumulator {
        private ColumnValue max = null;
        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) {
                if (max == null || val.compareTo(max) > 0) {
                    max = val;
                }
            }
        }
        @Override
        public Object getResult() { return max != null ? max.getValue() : null; }
    }

    private AggregateAccumulator createAccumulator(String expr) {
        String upper = expr.toUpperCase().trim();
        if (upper.startsWith("COUNT")) {
            String col = extractColumnName(expr);
            if ("*".equals(col) || col.isEmpty()) {
                return new CountStarAccumulator();
            } else {
                return new CountColumnAccumulator();
            }
        } else if (upper.startsWith("SUM")) {
            return new SumAccumulator();
        } else if (upper.startsWith("AVG")) {
            return new AvgAccumulator();
        } else if (upper.startsWith("MIN")) {
            return new MinAccumulator();
        } else if (upper.startsWith("MAX")) {
            return new MaxAccumulator();
        }
        throw new IllegalArgumentException("Unsupported aggregate expression: " + expr);
    }

    private BitSet evaluatePredicates(VectorizedScanner scanner, List<Predicate> predicates) {
        if (predicates == null || predicates.isEmpty()) {
            return null;
        }
        BitSet selection = null;
        for (Predicate pred : predicates) {
            BitSet filterBitSet = switch (pred.getOperator()) {
                case EQUALS -> scanner.filterEquals(pred.getColumnName(), pred.getTargetValue());
                case NOT_EQUALS -> scanner.filterNotEquals(pred.getColumnName(), pred.getTargetValue());
                case GREATER_THAN -> scanner.filterGreaterThan(pred.getColumnName(), pred.getTargetValue());
                case GREATER_THAN_OR_EQUALS -> scanner.filterGreaterThanOrEquals(pred.getColumnName(), pred.getTargetValue());
                case LESS_THAN -> scanner.filterLessThan(pred.getColumnName(), pred.getTargetValue());
                case LESS_THAN_OR_EQUALS -> scanner.filterLessThanOrEquals(pred.getColumnName(), pred.getTargetValue());
                case LIKE -> scanner.filterLike(pred.getColumnName(),
                        pred.getTargetValue() != null ? pred.getTargetValue().asString() : "");
            };

            if (filterBitSet != null) {
                if (selection == null) {
                    selection = filterBitSet;
                } else {
                    selection.and(filterBitSet);
                }
            }
        }
        return selection;
    }

    private QueryResult executeAggregateQuery(QueryRequest request, List<ColumnarChunk> chunks, long startNanos) {
        int scannedChunks = 0;
        int skippedChunks = 0;
        long totalRowsProcessed = 0;

        List<String> groupByCols = request.getGroupByColumns() != null ? request.getGroupByColumns() : Collections.emptyList();
        boolean hasGroupBy = !groupByCols.isEmpty();

        // If no group by: single global accumulator list
        List<AggregateAccumulator> globalAccumulators = new ArrayList<>();
        if (!hasGroupBy) {
            for (String expr : request.getSelectExpressions()) {
                globalAccumulators.add(createAccumulator(expr));
            }
        }

        // If group by: Map of GroupKey -> List of AggregateAccumulators
        Map<List<Object>, List<AggregateAccumulator>> groupMap = new LinkedHashMap<>();

        for (ColumnarChunk chunk : chunks) {
            // Predicate pushdown check using Zone Maps
            boolean canSkip = false;
            if (request.getPredicates() != null) {
                for (Predicate pred : request.getPredicates()) {
                    ZoneMap zm = chunk.getZoneMap(pred.getColumnName());
                    if (pred.canSkipChunk(zm)) {
                        canSkip = true;
                        break;
                    }
                }
            }

            if (canSkip) {
                skippedChunks++;
                continue;
            }

            scannedChunks++;
            totalRowsProcessed += chunk.getRowCount();
            VectorizedScanner scanner = new VectorizedScanner(chunk, batchSize);
            BitSet selection = evaluatePredicates(scanner, request.getPredicates());

            if (!hasGroupBy) {
                // Vectorized whole-chunk accumulation for global aggregates
                for (int i = 0; i < request.getSelectExpressions().size(); i++) {
                    String expr = request.getSelectExpressions().get(i);
                    AggregateAccumulator acc = globalAccumulators.get(i);
                    String col = extractColumnName(expr);

                    if (acc instanceof CountStarAccumulator csa) {
                        csa.count += scanner.aggregateCount(selection);
                    } else if (acc instanceof CountColumnAccumulator cca) {
                        cca.count += scanner.aggregateCountNonNull(col, selection);
                    } else if (acc instanceof SumAccumulator sa) {
                        if (scanner.isIntegerColumn(col)) {
                            sa.addLong(scanner.aggregateSumLong(col, selection));
                        } else {
                            sa.addDouble(scanner.aggregateSum(col, selection));
                        }
                    } else if (acc instanceof AvgAccumulator aa) {
                        aa.sum += scanner.aggregateSum(col, selection);
                        aa.count += scanner.aggregateCountNonNull(col, selection);
                    } else if (acc instanceof MinAccumulator ma) {
                        ColumnValue chunkMin = scanner.aggregateMin(col, selection);
                        if (chunkMin != null) ma.accumulate(chunkMin);
                    } else if (acc instanceof MaxAccumulator ma) {
                        ColumnValue chunkMax = scanner.aggregateMax(col, selection);
                        if (chunkMax != null) ma.accumulate(chunkMax);
                    }
                }
            } else {
                // Group by evaluation: extract column values for group keys and aggregates row by row
                List<List<ColumnValue>> groupColValues = new ArrayList<>();
                for (String gCol : groupByCols) {
                    groupColValues.add(chunk.getDecodedColumn(gCol));
                }

                List<List<ColumnValue>> aggColValues = new ArrayList<>();
                for (String expr : request.getSelectExpressions()) {
                    String col = extractColumnName(expr);
                    if ("*".equals(col) || col.isEmpty()) {
                        aggColValues.add(null);
                    } else {
                        aggColValues.add(chunk.getDecodedColumn(col));
                    }
                }

                int rowCount = chunk.getRowCount();
                for (int row = 0; row < rowCount; row++) {
                    if (selection != null && !selection.get(row)) {
                        continue;
                    }

                    List<Object> groupKey = new ArrayList<>(groupByCols.size());
                    for (List<ColumnValue> gList : groupColValues) {
                        ColumnValue cv = gList.get(row);
                        groupKey.add(cv != null ? cv.getValue() : null);
                    }

                    List<AggregateAccumulator> accs = groupMap.computeIfAbsent(groupKey, k -> {
                        List<AggregateAccumulator> list = new ArrayList<>(request.getSelectExpressions().size());
                        for (String expr : request.getSelectExpressions()) {
                            list.add(createAccumulator(expr));
                        }
                        return list;
                    });

                    for (int a = 0; a < request.getSelectExpressions().size(); a++) {
                        List<ColumnValue> colVals = aggColValues.get(a);
                        ColumnValue cv = (colVals != null) ? colVals.get(row) : null;
                        accs.get(a).accumulate(cv);
                    }
                }
            }
        }

        List<String> outputColumns = new ArrayList<>();
        if (hasGroupBy) {
            outputColumns.addAll(groupByCols);
        }
        outputColumns.addAll(request.getSelectExpressions());

        List<List<Object>> resultRows = new ArrayList<>();
        if (!hasGroupBy) {
            List<Object> row = new ArrayList<>();
            for (AggregateAccumulator acc : globalAccumulators) {
                row.add(acc.getResult());
            }
            resultRows.add(row);
        } else {
            for (Map.Entry<List<Object>, List<AggregateAccumulator>> entry : groupMap.entrySet()) {
                if (resultRows.size() >= request.getLimit()) break;
                List<Object> row = new ArrayList<>();
                row.addAll(entry.getKey());
                for (AggregateAccumulator acc : entry.getValue()) {
                    row.add(acc.getResult());
                }
                resultRows.add(row);
            }
        }

        double executionTimeMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        return new QueryResult(
                outputColumns,
                resultRows,
                executionTimeMs,
                totalRowsProcessed,
                scannedChunks,
                skippedChunks
        );
    }

    private QueryResult executeProjectQuery(QueryRequest request, List<ColumnarChunk> chunks, long startNanos) {
        int scannedChunks = 0;
        int skippedChunks = 0;
        long totalRowsProcessed = 0;
        List<List<Object>> rows = new ArrayList<>();

        for (ColumnarChunk chunk : chunks) {
            boolean canSkip = false;
            if (request.getPredicates() != null) {
                for (Predicate pred : request.getPredicates()) {
                    ZoneMap zm = chunk.getZoneMap(pred.getColumnName());
                    if (pred.canSkipChunk(zm)) {
                        canSkip = true;
                        break;
                    }
                }
            }

            if (canSkip) {
                skippedChunks++;
                continue;
            }

            scannedChunks++;
            totalRowsProcessed += chunk.getRowCount();
            VectorizedScanner scanner = new VectorizedScanner(chunk, batchSize);
            BitSet selection = evaluatePredicates(scanner, request.getPredicates());

            List<List<ColumnValue>> colLists = new ArrayList<>();
            for (String col : request.getSelectExpressions()) {
                colLists.add(chunk.getDecodedColumn(col));
            }

            int count = chunk.getRowCount();
            for (int i = 0; i < count && rows.size() < request.getLimit(); i++) {
                if (selection == null || selection.get(i)) {
                    List<Object> row = new ArrayList<>();
                    for (List<ColumnValue> colList : colLists) {
                        ColumnValue v = colList.get(i);
                        row.add(v != null ? v.getValue() : null);
                    }
                    rows.add(row);
                }
            }

            if (rows.size() >= request.getLimit()) {
                break;
            }
        }

        double executionTimeMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        return new QueryResult(
                request.getSelectExpressions(),
                rows,
                executionTimeMs,
                totalRowsProcessed,
                scannedChunks,
                skippedChunks
        );
    }

    private String extractColumnName(String expr) {
        int open = expr.indexOf('(');
        int close = expr.indexOf(')');
        if (open >= 0 && close > open) {
            return expr.substring(open + 1, close).trim();
        }
        return expr.trim();
    }
}
