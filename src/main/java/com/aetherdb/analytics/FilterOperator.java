package com.aetherdb.analytics;

/**
 * Comparison operators for analytical query filtering and predicate pushdown.
 */
public enum FilterOperator {
    EQUALS("="),
    NOT_EQUALS("!="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUALS(">="),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUALS("<="),
    LIKE("LIKE");

    private final String symbol;

    FilterOperator(String symbol) {
        this.symbol = symbol;
    }

    public String getSymbol() {
        return symbol;
    }

    public static FilterOperator fromString(String op) {
        for (FilterOperator f : values()) {
            if (f.symbol.equalsIgnoreCase(op) || f.name().equalsIgnoreCase(op)) {
                return f;
            }
        }
        return EQUALS;
    }
}
