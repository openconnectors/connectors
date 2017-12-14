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
import org.openconnectors.connect.PushSourceConnector;
import org.openconnectors.data.Record;
import org.openconnectors.source.AutoIncrementingJdbcQuerier;
import org.openconnectors.source.BulkJdbcQuerier;
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
    private static final long serialVersionUID = -3582091929013927187L;
    private static Logger logger = LoggerFactory.getLogger(JdbcSourceConnector.class);

    private Config config;
    private List<ConnectionProvider> connectionProviders = new ArrayList<>();
    private List<QuerierThread> workers = new ArrayList<>();
    private Consumer<Collection<Record>> consumeFunction;

    public void start() throws Exception {
        final String dbUrl = config.getString(JdbcConfig.Keys.CONNECTION_URL_CONFIG);
        final String dbUser = config.getString(JdbcConfig.Keys.CONNECTION_USER_CONFIG);
        final String dbPassword = config.getString(JdbcConfig.Keys.CONNECTION_PASSWORD_CONFIG);
        final int dbMaxConnectionAttempts = config.getInt(JdbcConfig.Keys.CONNECTION_MAX_ATTEMPT);
        final int dbConnectionRetryDelay = config.getInt(JdbcConfig.Keys.CONNECTION_RETRY_DELAY);
        final DbConnectionConfig connectionConfig = DbConnectionConfig.Builder
                .newBuilder()
                .setUrl(dbUrl)
                .setUser(dbUser)
                .setPassword(dbPassword)
                .setMaxConnectionAttempts(dbMaxConnectionAttempts)
                .setConnectionRetryDelay(dbConnectionRetryDelay)
                .build();
        ConnectionProvider connectionProvider = getConnectionProvider(connectionConfig);

        Connection connection = connectionProvider.getValidConnection();

        Map<String, List<String>> whiteListTables = getTablesAndColumns(JdbcConfig.Keys.WHITE_LIST_TABLES);
        Map<String, List<String>> blackListTables = getTablesAndColumns(JdbcConfig.Keys.BLACK_LIST_TABLES);
        if (!whiteListTables.isEmpty() && !blackListTables.isEmpty()) {
            String errorMsg = "Specify either \"white.list.tables\" or \"black.list.tables\", but not both";
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        Map<String, List<String>> tables = new HashMap<>();
        String schemaPattern = this.config.getString(JdbcConfig.Keys.SCHEMA_PATTERN);
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
        String mode = this.config.getString(JdbcConfig.Keys.MODE);
        Map<String, String> tableIncrementingColumnNames = new HashMap<>();
        if (mode.equals(JdbcConfig.Keys.INCREMENTING_MODE)) {
            String configLine = this.config.getString(JdbcConfig.Keys.INCREMENTING_COLUMN_NAME);
            tableIncrementingColumnNames = JdbcUtils.parseIncrementingColumns(configLine);
        }
        List<TableQuerier> tableQueriers = new ArrayList<>();
        for (String tableName : filteredTables.keySet()) {
            switch (mode) {
                case JdbcConfig.Keys.BULK_MODE:
                    tableQueriers.add(
                            new BulkJdbcQuerier(
                                    getConnectionProvider(connectionConfig),
                                    schemaPattern,
                                    tableName,
                                    filteredTables.get(tableName))
                    );
                    break;
                case JdbcConfig.Keys.INCREMENTING_MODE:
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
                                                            config.getLong(JdbcConfig.Keys.TABLE_CHECK_PERIOD),
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
            case JdbcConfig.Keys.BLACK_LIST_TABLES:
                return JdbcUtils.parseBlackListTables(configLine);
            case JdbcConfig.Keys.WHITE_LIST_TABLES:
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

    private JdbcSourceConnector(Builder builder) {
        config = builder.config;
    }

    public static class Builder {
        private Config config;

        public Builder withConfig(Config config) {
            this.config = config;
            return this;
        }

        public JdbcSourceConnector build() {
            if (config == null) {
                throw new IllegalArgumentException("You must provide a config using the withConfig method");
            }

            return new JdbcSourceConnector(this);
        }
    }
}
