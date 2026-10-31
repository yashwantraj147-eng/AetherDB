package com.aetherdb.model;

import com.aetherdb.common.BinaryUtils;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Table schema describing an ordered list of columns, with fast index lookups
 * and binary serialization for on-disk columnar chunks.
 * Enforces defensive bounds and validation against malformed or hostile metadata.
 */
public class Schema {

    public static final int MAX_COLUMNS = 10_000;
    public static final int MAX_COLUMN_NAME_LENGTH = 1_000;
    public static final int MAX_TABLE_NAME_LENGTH = 1_000;

    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final Map<String, Integer> columnIndexMap;

    public Schema(String tableName, List<ColumnDefinition> columns) {
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        if (tableName.length() > MAX_TABLE_NAME_LENGTH) {
            throw new IllegalArgumentException("Table name exceeds maximum length of " + MAX_TABLE_NAME_LENGTH);
        }
        Objects.requireNonNull(columns, "Columns list cannot be null");
        if (columns.size() > MAX_COLUMNS) {
            throw new IllegalArgumentException("Column count " + columns.size() + " exceeds maximum allowed " + MAX_COLUMNS);
        }

        Map<String, Integer> map = new HashMap<>();
        Set<String> seenNames = new HashSet<>();
        for (int i = 0; i < columns.size(); i++) {
            ColumnDefinition col = columns.get(i);
            Objects.requireNonNull(col, "ColumnDefinition at index " + i + " cannot be null");
            if (!seenNames.add(col.getName())) {
                throw new IllegalArgumentException("Duplicate column name in schema: '" + col.getName() + "'");
            }
            map.put(col.getName(), i);
        }
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        this.columnIndexMap = Collections.unmodifiableMap(map);
    }

    public String getTableName() { return tableName; }
    public List<ColumnDefinition> getColumns() { return columns; }
    public List<ColumnDefinition> getColumnDefinitions() { return columns; }
    public int getColumnCount() { return columns.size(); }

    public ColumnDefinition getColumn(int index) {
        return columns.get(index);
    }

    public ColumnDefinition getColumn(String name) {
        Integer idx = columnIndexMap.get(name);
        return (idx != null) ? columns.get(idx) : null;
    }

    public ColumnType getColumnType(String name) {
        ColumnDefinition col = getColumn(name);
        return col != null ? col.getType() : null;
    }

    public int getColumnIndex(String name) {
        Integer idx = columnIndexMap.get(name);
        if (idx == null) {
            throw new IllegalArgumentException("Column '" + name + "' not found in schema for table: " + tableName);
        }
        return idx;
    }

    public boolean hasColumn(String name) {
        return columnIndexMap.containsKey(name);
    }

    /**
     * Serializes schema to a ByteBuffer.
     */
    public void serialize(ByteBuffer buffer) {
        BinaryUtils.writeString(buffer, tableName);
        BinaryUtils.writeVarInt(buffer, columns.size());
        for (ColumnDefinition col : columns) {
            BinaryUtils.writeString(buffer, col.getName());
            buffer.put(col.getType().getTypeId());
        }
    }

    /**
     * Deserializes schema from a ByteBuffer with defensive limits and bounds checking.
     */
    public static Schema deserialize(ByteBuffer buffer) throws IOException {
        if (buffer == null || buffer.remaining() < 5) {
            throw new CorruptedSchemaException("Buffer underflow while reading schema header");
        }

        String tableName = readBoundedString(buffer, MAX_TABLE_NAME_LENGTH, "table name");
        int colCount = readVarIntSafe(buffer);

        if (colCount < 0) {
            throw new CorruptedSchemaException("Negative column count in schema: " + colCount);
        }
        if (colCount > MAX_COLUMNS) {
            throw new CorruptedSchemaException("Column count " + colCount + " exceeds maximum allowed: " + MAX_COLUMNS);
        }
        // Each column takes at least 2 bytes (name length + typeId)
        if (colCount > buffer.remaining() / 2) {
            throw new CorruptedSchemaException("Column count " + colCount + " exceeds remaining buffer capacity: " + buffer.remaining());
        }

        List<ColumnDefinition> cols = new ArrayList<>(colCount);
        Set<String> seenNames = new HashSet<>();

        for (int i = 0; i < colCount; i++) {
            String colName = readBoundedString(buffer, MAX_COLUMN_NAME_LENGTH, "column name");
            if (colName == null || colName.isEmpty()) {
                throw new CorruptedSchemaException("Empty column name at index " + i);
            }
            if (!seenNames.add(colName)) {
                throw new CorruptedSchemaException("Duplicate column name in schema: '" + colName + "'");
            }
            if (buffer.remaining() < 1) {
                throw new CorruptedSchemaException("Truncated schema: missing type identifier for column '" + colName + "'");
            }
            byte typeId = buffer.get();
            ColumnType type;
            try {
                type = ColumnType.fromTypeId(typeId);
            } catch (IllegalArgumentException e) {
                throw new CorruptedSchemaException("Invalid column type ID: " + typeId + " for column '" + colName + "'", e);
            }
            cols.add(new ColumnDefinition(colName, type));
        }
        return new Schema(tableName, cols);
    }

    private static String readBoundedString(ByteBuffer buffer, int maxLen, String fieldDesc) throws CorruptedSchemaException {
        int len;
        try {
            len = BinaryUtils.readVarInt(buffer);
        } catch (Exception e) {
            throw new CorruptedSchemaException("Malformed length for " + fieldDesc, e);
        }
        if (len < 0) {
            throw new CorruptedSchemaException("Negative length for " + fieldDesc + ": " + len);
        }
        if (len > maxLen) {
            throw new CorruptedSchemaException("Length " + len + " exceeds maximum allowed " + maxLen + " for " + fieldDesc);
        }
        if (len > buffer.remaining()) {
            throw new CorruptedSchemaException("Length " + len + " exceeds remaining buffer bytes " + buffer.remaining() + " for " + fieldDesc);
        }
        byte[] bytes = new byte[len];
        buffer.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private static int readVarIntSafe(ByteBuffer buffer) throws CorruptedSchemaException {
        try {
            return BinaryUtils.readVarInt(buffer);
        } catch (Exception e) {
            throw new CorruptedSchemaException("Malformed VarInt in schema", e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        if (!Objects.equals(tableName, schema.tableName)) return false;
        if (columns.size() != schema.columns.size()) return false;
        Map<String, ColumnType> thisMap = new HashMap<>();
        for (ColumnDefinition col : columns) {
            thisMap.put(col.getName(), col.getType());
        }
        for (ColumnDefinition col : schema.columns) {
            if (!thisMap.containsKey(col.getName()) || thisMap.get(col.getName()) != col.getType()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        Map<String, ColumnType> map = new TreeMap<>();
        for (ColumnDefinition col : columns) {
            map.put(col.getName(), col.getType());
        }
        return Objects.hash(tableName, map);
    }

    @Override
    public String toString() {
        return "Schema{" +
                "table='" + tableName + '\'' +
                ", columns=" + columns +
                '}';
    }
}
