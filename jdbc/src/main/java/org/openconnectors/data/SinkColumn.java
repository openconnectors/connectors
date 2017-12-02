package org.openconnectors.data;

import org.openconnectors.exceptions.SinkColumnException;

public class SinkColumn {
    private String columnName;
    private Object columnValue;
    private ColumnType columnType;
    private boolean isPrimaryKey;
    private boolean isUnique;
    private boolean isNullable;

    public SinkColumn(String columnName, Object columnValue, ColumnType columnType) {
        new SinkColumn(columnName, columnValue, columnType, false, false, true);
    }

    public SinkColumn(String columnName, Object columnValue, ColumnType columnType, boolean isPrimaryKey, boolean isUnique, boolean isNullable) {
        if (columnName == null || columnName.isEmpty()) {
            throw new SinkColumnException("Column name cannot be null or empty.");
        }
        if (columnType == null) {
            throw new SinkColumnException("Column type cannot be null.");
        }
        this.columnName = columnName;
        this.columnValue = columnValue;
        this.columnType = columnType;
        this.isPrimaryKey = isPrimaryKey;
        this.isUnique = isUnique;
        this.isNullable = isNullable;
    }

    public String getColumnName() {
        return columnName;
    }

    public Object getColumnValue() {
        return columnValue;
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    public boolean isPrimaryKey() {
        return isPrimaryKey;
    }

    public boolean isUnique() {
        return isUnique;
    }

    public boolean isNullable() {
        return isNullable;
    }
}
