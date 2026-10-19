package com.aetherdb.analytics;

import com.aetherdb.model.ColumnValue;
import com.aetherdb.storage.columnar.ZoneMap;

/**
 * Filter predicate condition over a column.
 */
public class Predicate {

    private final String columnName;
    private final FilterOperator operator;
    private final ColumnValue targetValue;

    public Predicate(String columnName, FilterOperator operator, ColumnValue targetValue) {
        this.columnName = columnName;
        this.operator = operator;
        this.targetValue = targetValue;
    }

    public String getColumnName() { return columnName; }
    public FilterOperator getOperator() { return operator; }
    public ColumnValue getTargetValue() { return targetValue; }

    /**
     * Checks if this entire chunk can be skipped using the ZoneMap without scanning rows.
     */
    public boolean canSkipChunk(ZoneMap zoneMap) {
        if (zoneMap == null) return false;

        return switch (operator) {
            case EQUALS -> zoneMap.canSkipEquals(targetValue);
            case GREATER_THAN, GREATER_THAN_OR_EQUALS -> zoneMap.canSkipGreaterThan(targetValue);
            case LESS_THAN, LESS_THAN_OR_EQUALS -> zoneMap.canSkipLessThan(targetValue);
            default -> false;
        };
    }
}
