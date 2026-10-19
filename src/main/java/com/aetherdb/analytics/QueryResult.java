package com.aetherdb.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Structured analytical query result with telemetry metrics and execution timing.
 */
public class QueryResult {

    private final List<String> columns;
    private final List<List<Object>> rows;
    private final double executionTimeMs;
    private final long totalRowsProcessed;
    private final int scannedChunks;
    private final int skippedChunks;

    public QueryResult(List<String> columns, List<List<Object>> rows, double executionTimeMs,
                       long totalRowsProcessed, int scannedChunks, int skippedChunks) {
        this.columns = columns != null ? Collections.unmodifiableList(columns) : Collections.emptyList();
        this.rows = rows != null ? Collections.unmodifiableList(rows) : Collections.emptyList();
        this.executionTimeMs = executionTimeMs;
        this.totalRowsProcessed = totalRowsProcessed;
        this.scannedChunks = scannedChunks;
        this.skippedChunks = skippedChunks;
    }

    public List<String> getColumns() { return columns; }
    public List<List<Object>> getRows() { return rows; }
    public double getExecutionTimeMs() { return executionTimeMs; }
    public long getTotalRowsProcessed() { return totalRowsProcessed; }
    public int getScannedChunks() { return scannedChunks; }
    public int getSkippedChunks() { return skippedChunks; }

    public int getRowCount() { return rows.size(); }
}
