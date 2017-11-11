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

package org.openconnectors.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class CachedConnectionProvider implements ConnectionProvider {

    private static final Logger logger = LoggerFactory.getLogger(CachedConnectionProvider.class);

    private static final int DB_CONNECTION_VALIDITY_TIMEOUT = 10;

    private DbConnectionConfig connectionConfig;
    private Connection connection;

    public CachedConnectionProvider(DbConnectionConfig connectionConfig) {
        if (connectionConfig == null) {
            throw new IllegalArgumentException("DbConnectionConfig cannot be null.");
        }
        this.connectionConfig = connectionConfig;
    }

    @Override
    public synchronized Connection getValidConnection() throws SQLException {
        if (!isConnectionValid()) {
            closeConnection();
            establishNewConnection();
        }
        return connection;
    }

    private boolean isConnectionValid() throws SQLException {
        return connection != null && connection.isValid(DB_CONNECTION_VALIDITY_TIMEOUT);
    }

    @Override
    public synchronized void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                logger.warn("SQLException has occurred during closing connection.", e);
            } finally {
                connection = null;
            }
        }
    }

    private void establishNewConnection() throws SQLException {
        int connectionAttempts = 0;
        int maxConnectionAttempts = connectionConfig.getMaxConnectionAttempts();
        while (connectionAttempts < maxConnectionAttempts) {
            connectionAttempts++;
            try {
                connection = DriverManager.getConnection(connectionConfig.getUrl(),
                                                         connectionConfig.getUser(),
                                                         connectionConfig.getPassword());
            } catch (SQLException sqle) {
                if (connectionAttempts < maxConnectionAttempts) {
                    String exceptionMsg = String.format("Unable to establish connection to database on attempt %d/%d. Will retry in %d seconds.",
                            connectionAttempts, maxConnectionAttempts, DB_CONNECTION_VALIDITY_TIMEOUT);
                    logger.info(exceptionMsg, sqle);
                    try {
                        Thread.sleep(connectionConfig.getConnectionRetryDelay() * 1000);
                    } catch (InterruptedException e) {
                        // do nothing
                    }
                } else {
                    throw sqle;
                }
            }
        }
    }
}
