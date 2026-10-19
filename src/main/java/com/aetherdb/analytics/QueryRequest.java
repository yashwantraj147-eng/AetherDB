package com.aetherdb.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Incoming request parameters for analytical columnar SQL queries.
 */
public class QueryRequest {

    private final String tableName;
    private final List<String> selectExpressions;
    private final List<Predicate> predicates;
    private final List<String> groupByColumns;
    private final int limit;

    public QueryRequest(String tableName,
                        List<String> selectExpressions,
                        List<Predicate> predicates,
                        List<String> groupByColumns,
                        int limit) {
        this.tableName = tableName;
        this.selectExpressions = selectExpressions != null ? Collections.unmodifiableList(selectExpressions) : Collections.emptyList();
        this.predicates = predicates != null ? Collections.unmodifiableList(predicates) : Collections.emptyList();
        this.groupByColumns = groupByColumns != null ? Collections.unmodifiableList(groupByColumns) : Collections.emptyList();
        this.limit = limit > 0 ? limit : 1000;
    }

    public String getTableName() { return tableName; }
    public List<String> getSelectExpressions() { return selectExpressions; }
    public List<Predicate> getPredicates() { return predicates; }
    public List<String> getGroupByColumns() { return groupByColumns; }
    public int getLimit() { return limit; }
}
