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

package org.openconnectors.cassandra;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.util.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.openconnectors.cassandra.CassandraConfigKeys.*;


/**
 * Simple AeroSpike sink
 */
public class CassandraSink<K, V> implements SinkConnector<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraSink.class);

    // ----- Runtime fields
    private Cluster cluster;
    private Session session;
    private String keySpace;
    private String keyName;
    private String columnFamily;
    private String columnName;
    private PreparedStatement statement;

    @Override
    public void initialize(ConnectorContext ctx) {
        // Nothing really
    }

    @Override
    public void open(Config config) throws Exception {
        verifyExists(config, CASSANDRA_ROOTS);
        verifyExists(config, CASSANDRA_KEYSPACE);
        verifyExists(config, CASSANDRA_KEYNAME);
        verifyExists(config, CASSANDRA_COLUMNFAMILY);
        verifyExists(config, CASSANDRA_COLUMNNAME);
        keySpace = config.getString(CASSANDRA_KEYSPACE);
        keyName = config.getString(CASSANDRA_KEYNAME);
        columnFamily = config.getString(CASSANDRA_COLUMNFAMILY);
        columnName = config.getString(CASSANDRA_COLUMNNAME);
        createClient(config);
        statement = session.prepare("INSERT INTO " + columnFamily + " ("
                + keyName + ", " + columnName + ") VALUES (?, ?)");
    }

    @Override
    public void close() throws Exception {
        session.close();
        cluster.close();
    }

    @Override
    public String getVersion() {
        return CASSANDRA_CONNECTOR_VERSION;
    }

    @Override
    public CompletableFuture<Void> publish(Collection<KeyValue<K, V>> tuples) {
        for (KeyValue<K, V> tuple : tuples) {
            BoundStatement bound = statement.bind(tuple.getKey(), tuple.getValue());
            session.execute(bound);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() { }

    private void verifyExists(Config config, String key) {
        if (config.getString(key) == null) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    private void createClient(Config config) {
        String[] hosts = config.getString(CASSANDRA_ROOTS).split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid cassandra roots");
        }
        Cluster.Builder b = Cluster.builder();
        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            b.addContactPoint(hostPort[0]);
            if (hostPort.length > 1) {
                b.withPort(Integer.valueOf(hostPort[1]));
            }
        }
        cluster = b.build();
        session = cluster.connect();
        session.execute("USE " + keySpace);
    }
}
