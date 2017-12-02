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

package org.openconnectors;

import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.openconnectors.data.Record;
import org.openconnectors.source.BulkJdbcQuerier;
import org.openconnectors.source.AutoIncrementingJdbcQuerier;
import org.openconnectors.source.QuerierThread;
import org.openconnectors.source.TableQuerier;
import org.openconnectors.util.CachedConnectionProvider;
import org.openconnectors.util.ConnectionProvider;
import org.openconnectors.util.DbConnectionConfig;
import org.openconnectors.util.JdbcUtils;
import org.openconnectors.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class JdbcSourceConnector implements PushSourceConnector<Record> {

    private static Logger logger = LoggerFactory.getLogger(JdbcSourceConnector.class);

    private Config config;
    private List<ConnectionProvider> connectionProviders = new ArrayList<>();
    private List<QuerierThread> workers = new ArrayList<>();
    private Consumer<Collection<Record>> consumeFunction;

    private void configureConnector(Config config) {
        // TODO: validate config
        this.config = config;
    }


    @Override
    public void initialize(ConnectorContext ctx) {

    }

    @Override
    public void open(Config config) throws Exception {
        configureConnector(config);
        final DbConnectionConfig connectionConfig = JdbcUtils.getConnectionConfig(config);
        ConnectionProvider connectionProvider = getConnectionProvider(connectionConfig);

        Connection connection = connectionProvider.getValidConnection();

        Map<String, List<String>> whiteListTables = getTablesAndColumns(JdbcConfigKeys.WHITE_LIST_TABLES);
        Map<String, List<String>> blackListTables = getTablesAndColumns(JdbcConfigKeys.BLACK_LIST_TABLES);
        if (!whiteListTables.isEmpty() && !blackListTables.isEmpty()) {
            String errorMsg = "Specify either \"white.list.tables\" or \"black.list.tables\", but not both";
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        Map<String, List<String>> tables = new HashMap<>();
        String schemaPattern = this.config.getString(JdbcConfigKeys.SCHEMA_PATTERN);
        // TODO: handle types[] parameter
        ResultSet tablesResultSet = connection.getMetaData().getTables(null, schemaPattern, "%", null);
        final int tableNameColumnIndex = 3;
        while (tablesResultSet.next()) {
            String tableName = tablesResultSet.getString(tableNameColumnIndex).toLowerCase();
            tables.put(tableName, null);
        }

        Map<String, List<String>> filteredTables = new HashMap<>();
        if (!whiteListTables.isEmpty()) {
            whiteListTables
                .keySet()
                .stream()
                .filter(tables::containsKey)
                .forEach(tableName -> filteredTables.put(tableName, whiteListTables.get(tableName)));
        }

        if (!blackListTables.isEmpty()) {
            tables
                .keySet()
                .stream()
                .filter(tableName -> !blackListTables.containsKey(tableName))
                .forEach(tableName -> filteredTables.put(tableName, null));
        }
        String mode = this.config.getString(JdbcConfigKeys.MODE);
        Map<String, String> tableIncrementingColumnNames = new HashMap<>();
        if (mode.equals(JdbcConfigKeys.INCREMENTING_MODE)) {
            String configLine = this.config.getString(JdbcConfigKeys.INCREMENTING_COLUMN_NAME);
            tableIncrementingColumnNames = JdbcUtils.parseIncrementingColumns(configLine);
        }
        List<TableQuerier> tableQueriers = new ArrayList<>();
        for (String tableName : filteredTables.keySet()) {
            switch (mode) {
                case JdbcConfigKeys.BULK_MODE:
                    tableQueriers.add(
                            new BulkJdbcQuerier(
                                    getConnectionProvider(connectionConfig),
                                    schemaPattern,
                                    tableName,
                                    filteredTables.get(tableName))
                    );
                    break;
                case JdbcConfigKeys.INCREMENTING_MODE:
                    tableQueriers.add(
                            new AutoIncrementingJdbcQuerier(
                                    getConnectionProvider(connectionConfig),
                                    schemaPattern,
                                    tableName,
                                    filteredTables.get(tableName),
                                    tableIncrementingColumnNames.get(tableName)
                            )
                    );
                    break;
                default:
                    throw new UnsupportedOperationException("Currently, only BULK mode is supported");
            }
        }
        for(TableQuerier tableQuerier : tableQueriers) {
            QuerierThread querierThread = new QuerierThread(tableQuerier,
                                                            config.getLong(JdbcConfigKeys.TABLE_CHECK_PERIOD),
                                                            consumeFunction);
            querierThread.start();
            workers.add(querierThread);
        }
    }

    @Override
    public void close() throws Exception {
        workers.forEach(QuerierThread::stopQuerying);
        connectionProviders.forEach(ConnectionProvider::closeConnection);
    }

    @Override
    public String getVersion() {
        return Version.getVersion();
    }

    @Override
    public void setConsumer(Consumer<Collection<Record>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    private Map<String, List<String>> getTablesAndColumns(String tableConfigKey) {
        String configLine = config.getString(tableConfigKey);
        if (configLine == null || configLine.isEmpty()) {
            return new HashMap<>(0);
        }
        switch (tableConfigKey) {
            case JdbcConfigKeys.BLACK_LIST_TABLES:
                return JdbcUtils.parseBlackListTables(configLine);
            case JdbcConfigKeys.WHITE_LIST_TABLES:
                return JdbcUtils.parseWhiteListTablesAndColumns(configLine);
            default:
                return new HashMap<>(0);
        }
    }

    private ConnectionProvider getConnectionProvider(DbConnectionConfig connectionConfig) {
        ConnectionProvider connectionProvider = new CachedConnectionProvider(connectionConfig);
        connectionProviders.add(connectionProvider);
        return connectionProvider;
    }
}
