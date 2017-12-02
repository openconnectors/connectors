/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.openconnectors.data;

import java.util.HashMap;
import java.util.Map;

public class TableMetaData {

    private String tableName;
    private int columnsCount;
    private Map<String, Column> columnsByName;
    private Map<Integer, Column> columnsByIndex;

    public String getTableName() {
        return tableName;
    }

    public Map<String, Column> getColumnsByName() {
        return columnsByName;
    }

    public Map<Integer, Column> getColumnsByIndex() {
        return columnsByIndex;
    }

    public Column getColumnByIndex(int columnIndex) {
        return columnsByIndex.get(columnIndex);
    }

    public Column getColumnByName(String name) {
        return columnsByName.get(name);
    }

    public int getColumnsCount() {
        return columnsCount;
    }

    private TableMetaData(String tableName, int columnsCount) {
        this.tableName = tableName;
        this.columnsCount = columnsCount;
        columnsByIndex = new HashMap<>();
        columnsByName = new HashMap<>();
    }

    public static class Column {
        private String name;
        private int index;
        private ColumnType columnType;

        public Column(String name, int index, ColumnType columnType) {
            this.name = name;
            this.index = index;
            this.columnType = columnType;
        }

        public String getName() {
            return name;
        }

        public int getIndex() {
            return index;
        }
    }

    public static class TableMetaDataBuilder {
        private TableMetaData tableMetaData;

        public TableMetaDataBuilder(String tableName, int columnsCount) {
            tableMetaData = new TableMetaData(tableName, columnsCount);
        }

        public TableMetaDataBuilder addColumn(int index, String name, ColumnType type) {
            Column column = new Column(name, index, type);
            tableMetaData.columnsByName.put(name, column);
            tableMetaData.columnsByIndex.put(index, column);
            return this;
        }

        public TableMetaData build() {
            return tableMetaData;
        }

    }
}
