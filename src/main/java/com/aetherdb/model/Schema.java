package com.aetherdb.model;

import com.aetherdb.common.BinaryUtils;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Table schema describing an ordered list of columns, with fast index lookups
 * and binary serialization for on-disk columnar chunks.
 */
public class Schema {

    private final String tableName;
    private final List<ColumnDefinition> columns;
    private final Map<String, Integer> columnIndexMap;

    public Schema(String tableName, List<ColumnDefinition> columns) {
        this.tableName = Objects.requireNonNull(tableName, "Table name cannot be null");
        this.columns = Collections.unmodifiableList(new ArrayList<>(columns));
        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i).getName(), i);
        }
        this.columnIndexMap = Collections.unmodifiableMap(map);
    }

    public String getTableName() { return tableName; }
    public List<ColumnDefinition> getColumns() { return columns; }
    public int getColumnCount() { return columns.size(); }

    public ColumnDefinition getColumn(int index) {
        return columns.get(index);
    }

    public ColumnDefinition getColumn(String name) {
        Integer idx = columnIndexMap.get(name);
        return (idx != null) ? columns.get(idx) : null;
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
            buffer.put((byte) (col.isNullable() ? 1 : 0));
        }
    }

    /**
     * Deserializes schema from a ByteBuffer.
     */
    public static Schema deserialize(ByteBuffer buffer) {
        String tableName = BinaryUtils.readString(buffer);
        int colCount = BinaryUtils.readVarInt(buffer);
        List<ColumnDefinition> cols = new ArrayList<>(colCount);
        for (int i = 0; i < colCount; i++) {
            String colName = BinaryUtils.readString(buffer);
            byte typeId = buffer.get();
            boolean nullable = buffer.get() == 1;
            cols.add(new ColumnDefinition(colName, ColumnType.fromTypeId(typeId), nullable));
        }
        return new Schema(tableName, cols);
    }

    @Override
    public String toString() {
        return "Schema[" + tableName + ": " + columns + "]";
    }
}
