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
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.data.SinkRecord;
import org.openconnectors.sink.DbWriter;
import org.openconnectors.sink.JdbcDbWriter;
import org.openconnectors.util.DbConnectionConfig;
import org.openconnectors.util.JdbcUtils;
import org.openconnectors.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class JdbcSinkConnector implements SinkConnector<SinkRecord> {

    private static Logger logger = LoggerFactory.getLogger(JdbcSourceConnector.class);

    private DbWriter writer;

    @Override
    public String getVersion() {
        return Version.getVersion();
    }

    @Override
    public void initialize(ConnectorContext ctx) {
    }

    @Override
    public void open(Config config) throws Exception {
        DbConnectionConfig dbConnectionConfig = JdbcUtils.getConnectionConfig(config);
        writer = new JdbcDbWriter(dbConnectionConfig);
    }

    @Override
    public CompletableFuture<Void> publish(Collection<SinkRecord> messages) {
        List<SinkRecord> recordsList = new ArrayList<>();
        recordsList.addAll(messages);
        try {
            writer.write(recordsList);
        } catch (SQLException e) {
            try {
                writer.close();
            } catch (SQLException e1) {
                logger.error("Unable to close writer.", e1);
            }
            logger.error("Unable to write records.", e);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() throws Exception {

    }

    @Override
    public void close() throws Exception {
        writer.close();
    }

}
