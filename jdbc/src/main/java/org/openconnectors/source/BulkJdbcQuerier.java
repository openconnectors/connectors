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

package org.openconnectors.source;

import org.openconnectors.util.ConnectionProvider;
import org.openconnectors.util.JdbcUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class BulkJdbcQuerier extends TableQuerier {

    private List<String> columnsToQuery;

    public BulkJdbcQuerier(ConnectionProvider connectionProvider, String schemaPattern, String tableName,
                           List<String> columnsToQuery) {
        super(connectionProvider, schemaPattern, tableName);
        this.columnsToQuery = columnsToQuery;
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        return getPreparedStatement().executeQuery();
    }

    @Override
    protected void updateCursor(int newRecordsNumber) {
        // nothing to do
    }

    @Override
    protected PreparedStatement createPreparedStatement() throws SQLException {
        String identifierQuoteString = getConnection().getMetaData().getIdentifierQuoteString();
        String fromStatement = JdbcUtils.buildFromStatement(tableName, identifierQuoteString);
        String columnsString = JdbcUtils.buildColumnsListString(columnsToQuery);
        String sqlString = "SELECT " + columnsString + " " + fromStatement;
        return getConnection().prepareStatement(sqlString);
    }
}
