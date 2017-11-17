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

import com.google.common.collect.Sets;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.openconnectors.data.Record;
import org.openconnectors.source.BulkJdbcQuerier;
import org.openconnectors.source.QuerierThread;
import org.openconnectors.source.TableQuerier;
import org.openconnectors.util.CachedConnectionProvider;
import org.openconnectors.util.ConnectionProvider;
import org.openconnectors.util.DbConnectionConfig;
import org.openconnectors.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        final String dbUrl = config.getString(JdbcConfigKeys.CONNECTION_URL_CONFIG);
        final String dbUser = config.getString(JdbcConfigKeys.CONNECTION_USER_CONFIG);
        final String dbPassword = config.getString(JdbcConfigKeys.CONNECTION_PASSWORD_CONFIG);
        final int dbMaxConnectionAttempts = config.getInt(JdbcConfigKeys.CONNECTION_MAX_ATTEMPT);
        final int dbConnectionRetryDelay = config.getInt(JdbcConfigKeys.CONNECTION_RETRY_DELAY);
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

        Set<String> whiteListTables = getWhiteListTables();
        Set<String> blackListTables = getBlackListTables();
        if (!whiteListTables.isEmpty() && !blackListTables.isEmpty()) {
            String errorMsg = "Specify either \"white.list.tables\" or \"black.list.tables\", but not both";
            logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }
        List<String> tables = new ArrayList<>();
        String schemaPattern = this.config.getString(JdbcConfigKeys.SCHEMA_PATTERN);
        // TODO: handle types[] parameter
        ResultSet tablesResultSet = connection.getMetaData().getTables(null, schemaPattern, "%", null);
        final int tableNameColumnIndex = 3;
        while (tablesResultSet.next()) {
            String tableName = tablesResultSet.getString(tableNameColumnIndex).toLowerCase();
            tables.add(tableName);
        }

        List<String> filteredTables = new ArrayList<>();
        if (!whiteListTables.isEmpty()) {
            filteredTables.addAll(tables.stream()
                    .filter(whiteListTables::contains)
                    .collect(Collectors.toList())
            );
        }

        if (!blackListTables.isEmpty()) {
            filteredTables.addAll(tables.stream()
                    .filter(table -> !blackListTables.contains(table))
                    .collect(Collectors.toList())
            );
        }
        String mode = this.config.getString(JdbcConfigKeys.MODE);
        List<TableQuerier> tableQueriers = new ArrayList<>();
        for (String tableName : filteredTables) {
            switch (mode) {
                case JdbcConfigKeys.BULK_MODE:
                    tableQueriers.add(
                            new BulkJdbcQuerier(getConnectionProvider(connectionConfig), schemaPattern, tableName)
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

    private Set<String> getWhiteListTables() {
        return getTables(JdbcConfigKeys.WHITE_LIST_TABLES);
    }

    private Set<String> getBlackListTables() {
        return getTables(JdbcConfigKeys.BLACK_LIST_TABLES);
    }

    private Set<String> getTables(String listName) {
        Set<String> tables = new HashSet<>();
        if (listName == null || listName.isEmpty()) {
            return tables;
        }
        listName = config.getString(listName);
        if (listName == null || listName.isEmpty()) {
            return tables;
        }
        tables = Stream
            .of(listName.split(","))
            .map(t -> t.trim().toLowerCase())
            .collect(Collectors.toSet());
        if (tables == null) {
            return Sets.newHashSet();
        }
        return tables;
    }

    private ConnectionProvider getConnectionProvider(DbConnectionConfig connectionConfig) {
        ConnectionProvider connectionProvider = new CachedConnectionProvider(connectionConfig);
        connectionProviders.add(connectionProvider);
        return connectionProvider;
    }
}
