package com.aetherdb.analytics;

/**
 * Analytical aggregation functions supported by the vectorized query engine.
 */
public enum AggregateFunction {
    SUM,
    AVG,
    MIN,
    MAX,
    COUNT;

    public static AggregateFunction fromString(String func) {
        for (AggregateFunction af : values()) {
            if (af.name().equalsIgnoreCase(func)) {
                return af;
            }
        }
        return COUNT;
    }
}
