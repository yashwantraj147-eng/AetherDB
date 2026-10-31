package com.aetherdb.analytics;

import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.columnar.ColumnarChunk;
import com.aetherdb.storage.columnar.VectorizedScanner;
import com.aetherdb.storage.columnar.ZoneMap;
import com.aetherdb.storage.engine.StorageEngine;

import java.util.*;

/**
 * Analytical Query Planner & Execution Engine.
 * Implements:
 * 1. Predicate pushdown with ZoneMap data skipping (bypassing unneeded chunks).
 * 2. Vectorized column evaluations and batch aggregations with independent aggregate state per expression.
 * 3. Robust GROUP BY support with group-specific aggregation state.
 * 4. Correct inclusive boundary filtering and null semantics.
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
        List<ColumnarChunk> chunks = storageEngine.getColumnarChunks(request.getTableName());

        if (chunks.isEmpty()) {
            List<String> outputCols = new ArrayList<>();
            if (request.getGroupByColumns() != null) {
                outputCols.addAll(request.getGroupByColumns());
            }
            outputCols.addAll(request.getSelectExpressions());
            return new QueryResult(outputCols, Collections.emptyList(), 0.0, 0, 0, 0);
        }

        boolean hasGroupBy = request.getGroupByColumns() != null && !request.getGroupByColumns().isEmpty();
        boolean isAggregateQuery = hasGroupBy;

        if (!isAggregateQuery) {
            for (String expr : request.getSelectExpressions()) {
                String upper = expr.toUpperCase().trim();
                if (upper.startsWith("COUNT") || upper.startsWith("SUM") || upper.startsWith("AVG")
                        || upper.startsWith("MIN") || upper.startsWith("MAX")) {
                    isAggregateQuery = true;
                    break;
                }
            }
        }

        if (isAggregateQuery) {
            return executeAggregateQuery(request, chunks, startNanos);
        } else {
            return executeProjectQuery(request, chunks, startNanos);
        }
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
        private double sum = 0.0;
        private boolean hasValue = false;
        @Override
        public void accumulate(ColumnValue val) {
            if (val != null && !val.isNull()) {
                sum += val.asDouble();
                hasValue = true;
            }
        }
        @Override
        public Object getResult() { return hasValue ? sum : 0.0; }
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
        public Object getResult() { return count > 0 ? (sum / count) : 0.0; }
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
        public Object getResult() { return min != null ? min.getValue() : "NULL"; }
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
        public Object getResult() { return max != null ? max.getValue() : "NULL"; }
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
        // Fallback for non-aggregate expression in group-by query
        return new MaxAccumulator();
    }

    private BitSet evaluatePredicates(VectorizedScanner scanner, List<Predicate> predicates) {
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
            for (Predicate pred : request.getPredicates()) {
                ZoneMap zm = chunk.getZoneMap(pred.getColumnName());
                if (pred.canSkipChunk(zm)) {
                    canSkip = true;
                    break;
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
                        sa.sum += scanner.aggregateSum(col, selection);
                        sa.hasValue = true;
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
            for (Predicate pred : request.getPredicates()) {
                ZoneMap zm = chunk.getZoneMap(pred.getColumnName());
                if (pred.canSkipChunk(zm)) {
                    canSkip = true;
                    break;
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
