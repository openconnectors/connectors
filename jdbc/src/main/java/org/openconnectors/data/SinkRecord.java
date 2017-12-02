package org.openconnectors.data;

import org.openconnectors.exceptions.SinkRecordException;

import java.util.Map;

public class SinkRecord {
    private String schemaName;
    private String tableName;
    private Map<String, SinkColumn> columns;

    public SinkRecord(String schemaName, String tableName, Map<String, SinkColumn> columns) {
        if (tableName == null || tableName.isEmpty()) {
            throw new SinkRecordException("Table name cannot be null or empty.");
        }
        if (columns == null || columns.isEmpty()) {
            throw new SinkRecordException("Column parameter cannot be null or empty.");
        }
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.columns = columns;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public Map<String, SinkColumn> getColumns() {
        return columns;
    }
}
