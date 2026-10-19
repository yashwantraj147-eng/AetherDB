package com.aetherdb.model;

import java.util.Objects;

/**
 * Definition of an individual column in a columnar dataset.
 */
public class ColumnDefinition {

    private final String name;
    private final ColumnType type;
    private final boolean nullable;

    public ColumnDefinition(String name, ColumnType type, boolean nullable) {
        this.name = Objects.requireNonNull(name, "Column name cannot be null");
        this.type = Objects.requireNonNull(type, "Column type cannot be null");
        this.nullable = nullable;
    }

    public ColumnDefinition(String name, ColumnType type) {
        this(name, type, true);
    }

    public String getName() { return name; }
    public ColumnType getType() { return type; }
    public boolean isNullable() { return nullable; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnDefinition that = (ColumnDefinition) o;
        return nullable == that.nullable &&
               Objects.equals(name, that.name) &&
               type == that.type;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, nullable);
    }

    @Override
    public String toString() {
        return name + " (" + type + (nullable ? ", NULL" : ", NOT NULL") + ")";
    }
}
