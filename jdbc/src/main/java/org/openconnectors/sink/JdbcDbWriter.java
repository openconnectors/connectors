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

package org.openconnectors.sink;

import org.openconnectors.data.SinkColumn;
import org.openconnectors.data.SinkRecord;
import org.openconnectors.util.CachedConnectionProvider;
import org.openconnectors.util.ConnectionProvider;
import org.openconnectors.util.DbConnectionConfig;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JdbcDbWriter implements DbWriter {
    private DbDialect dbDialect;
    private ConnectionProvider connectionProvider;
    private List<String> columns;
    private PreparedStatement preparedStatement;

    public JdbcDbWriter(DbConnectionConfig connectionConfig) {
        this.connectionProvider = new CachedConnectionProvider(connectionConfig);
        this.dbDialect = new GenericDbDialect();
    }

    @Override
    public void write(List<SinkRecord> records) throws SQLException {
        if (!records.isEmpty()) {
            if (preparedStatement == null) {
                SinkRecord record = records.get(0);
                Set<String> columnNamesSet = record.getColumns().keySet();
                setUpPreparedStatement(record.getTableName(), columnNamesSet);
            }
            for (SinkRecord r : records) {
                Map<String, SinkColumn> columns = r.getColumns();
                for (int i = 0; i < this.columns.size(); i++) {
                    String columnName = this.columns.get(i);
                    Object columnValue = columns.get(columnName).getColumnValue();
                    preparedStatement.setObject(i + 1, columnValue);
                }
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
        }
    }

    @Override
    public void close() throws SQLException {
        if (preparedStatement != null) {
            preparedStatement.close();
            preparedStatement = null;
        }
        connectionProvider.closeConnection();
    }

    private void setUpPreparedStatement(String tableName, Set<String> columns) throws SQLException {
        if (preparedStatement == null) {
            Connection connection = connectionProvider.getValidConnection();
            this.columns = new ArrayList<>();
            this.columns.addAll(columns);
            String insertStatement = dbDialect.getInsertStatement(tableName, this.columns);
            preparedStatement = connection.prepareStatement(insertStatement);
        }
    }

}
