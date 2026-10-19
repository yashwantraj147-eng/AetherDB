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
 * 2. Vectorized column evaluations and batch aggregations.
 * 3. Execution time profiling and telemetry.
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
            return new QueryResult(request.getSelectExpressions(), Collections.emptyList(), 0.0, 0, 0, 0);
        }

        int scannedChunks = 0;
        int skippedChunks = 0;
        long totalRows = 0;

        // Check if query is a simple aggregation (e.g., COUNT(*), SUM(x), AVG(x), MIN(x), MAX(x))
        boolean isAggregateQuery = false;
        for (String expr : request.getSelectExpressions()) {
            String upper = expr.toUpperCase().trim();
            if (upper.startsWith("COUNT") || upper.startsWith("SUM") || upper.startsWith("AVG")
                    || upper.startsWith("MIN") || upper.startsWith("MAX")) {
                isAggregateQuery = true;
                break;
            }
        }

        if (isAggregateQuery) {
            return executeAggregateQuery(request, chunks, startNanos);
        } else {
            return executeProjectQuery(request, chunks, startNanos);
        }
    }

    private QueryResult executeAggregateQuery(QueryRequest request, List<ColumnarChunk> chunks, long startNanos) {
        int scannedChunks = 0;
        int skippedChunks = 0;
        long totalRowsProcessed = 0;

        double sumResult = 0.0;
        long countResult = 0;
        ColumnValue minResult = null;
        ColumnValue maxResult = null;

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

            // Evaluate predicates to create selection bitset
            BitSet selection = null;
            for (Predicate pred : request.getPredicates()) {
                BitSet filterBitSet = switch (pred.getOperator()) {
                    case EQUALS -> scanner.filterEquals(pred.getColumnName(), pred.getTargetValue());
                    case GREATER_THAN, GREATER_THAN_OR_EQUALS -> scanner.filterGreaterThan(pred.getColumnName(), pred.getTargetValue());
                    case LESS_THAN, LESS_THAN_OR_EQUALS -> scanner.filterLessThan(pred.getColumnName(), pred.getTargetValue());
                    default -> null;
                };

                if (filterBitSet != null) {
                    if (selection == null) {
                        selection = filterBitSet;
                    } else {
                        selection.and(filterBitSet);
                    }
                }
            }

            // Run aggregations
            for (String expr : request.getSelectExpressions()) {
                String upper = expr.toUpperCase().trim();
                String col = extractColumnName(expr);

                if (upper.startsWith("COUNT")) {
                    countResult += scanner.aggregateCount(selection);
                } else if (upper.startsWith("SUM")) {
                    sumResult += scanner.aggregateSum(col, selection);
                    countResult += scanner.aggregateCount(selection);
                } else if (upper.startsWith("AVG")) {
                    sumResult += scanner.aggregateSum(col, selection);
                    countResult += scanner.aggregateCount(selection);
                } else if (upper.startsWith("MIN")) {
                    ColumnValue chunkMin = scanner.aggregateMin(col, selection);
                    if (chunkMin != null && (minResult == null || chunkMin.compareTo(minResult) < 0)) {
                        minResult = chunkMin;
                    }
                } else if (upper.startsWith("MAX")) {
                    ColumnValue chunkMax = scanner.aggregateMax(col, selection);
                    if (chunkMax != null && (maxResult == null || chunkMax.compareTo(maxResult) > 0)) {
                        maxResult = chunkMax;
                    }
                }
            }
        }

        List<Object> resultRow = new ArrayList<>();
        for (String expr : request.getSelectExpressions()) {
            String upper = expr.toUpperCase().trim();
            if (upper.startsWith("COUNT")) {
                resultRow.add(countResult);
            } else if (upper.startsWith("SUM")) {
                resultRow.add(sumResult);
            } else if (upper.startsWith("AVG")) {
                resultRow.add(countResult > 0 ? (sumResult / countResult) : 0.0);
            } else if (upper.startsWith("MIN")) {
                resultRow.add(minResult != null ? minResult.getValue() : "NULL");
            } else if (upper.startsWith("MAX")) {
                resultRow.add(maxResult != null ? maxResult.getValue() : "NULL");
            }
        }

        double executionTimeMs = (System.nanoTime() - startNanos) / 1_000_000.0;
        return new QueryResult(
                request.getSelectExpressions(),
                List.of(resultRow),
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

            BitSet selection = null;
            for (Predicate pred : request.getPredicates()) {
                BitSet filterBitSet = switch (pred.getOperator()) {
                    case EQUALS -> scanner.filterEquals(pred.getColumnName(), pred.getTargetValue());
                    case GREATER_THAN, GREATER_THAN_OR_EQUALS -> scanner.filterGreaterThan(pred.getColumnName(), pred.getTargetValue());
                    case LESS_THAN, LESS_THAN_OR_EQUALS -> scanner.filterLessThan(pred.getColumnName(), pred.getTargetValue());
                    default -> null;
                };

                if (filterBitSet != null) {
                    if (selection == null) selection = filterBitSet;
                    else selection.and(filterBitSet);
                }
            }

            List<List<ColumnValue>> colLists = new ArrayList<>();
            for (String col : request.getSelectExpressions()) {
                colLists.add(chunk.getDecodedColumn(col));
            }

            int rowLimit = request.getLimit() - rows.size();
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
