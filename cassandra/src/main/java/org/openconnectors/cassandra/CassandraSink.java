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
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.util.KeyValue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.openconnectors.cassandra.CassandraConfig.CASSANDRA_CONNECTOR_VERSION;

/**
 * Simple Cassandra sink
 */
public class CassandraSink<K, V> implements SinkConnector<KeyValue<K, V>> {
    private static final long serialVersionUID = -6315650519679320003L;
    private PreparedStatement statement;
    private Cluster cluster;
    private Session session;

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
    public void flush() {
    }

    private CassandraSink(Builder builder) {
        String[] hosts = builder.roots.split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid cassandra roots");
        }
        Cluster.Builder clusterBuilder = Cluster.builder();
        for (String host : hosts) {
            String[] hostPort = host.split(":");
            clusterBuilder.addContactPoint(hostPort[0]);
            if (hostPort.length > 1) {
                clusterBuilder.withPort(Integer.valueOf(hostPort[1]));
            }
        }
        cluster = clusterBuilder.build();

        String rawStatement = String.format("INSERT INTO %s (%s, %s) VALUES (?, ?)",
                builder.columnFamily,
                builder.keyName,
                builder.columnName);

        session = cluster.connect();
        statement = session.prepare(rawStatement);
        session.execute(String.format("USE %s", builder.keyspace));
    }

    public static final class Builder<K, V> {
        private String roots, keyspace, keyName, columnFamily, columnName;

        Builder() {}

        public Builder<K, V> setRoots(String roots) {
            this.roots = roots;
            return this;
        }

        public Builder<K, V> setKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder<K, V> setKeyName(String keyName) {
            this.keyName = keyName;
            return this;
        }

        public Builder<K, V> setColumnFamily(String columnFamily) {
            this.columnFamily = columnFamily;
            return this;
        }

        public Builder<K, V> setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder<K, V> usingConfigProvider(Config config) {
            config.verify(
                    CassandraConfig.Keys.CASSANDRA_ROOTS,
                    CassandraConfig.Keys.CASSANDRA_COLUMNFAMILY,
                    CassandraConfig.Keys.CASSANDRA_COLUMNNAME,
                    CassandraConfig.Keys.CASSANDRA_KEYNAME,
                    CassandraConfig.Keys.CASSANDRA_KEYSPACE
            );
            this.roots = config.getString(CassandraConfig.Keys.CASSANDRA_ROOTS);
            this.columnFamily = config.getString(CassandraConfig.Keys.CASSANDRA_COLUMNFAMILY);
            this.columnName = config.getString(CassandraConfig.Keys.CASSANDRA_COLUMNNAME);
            this.keyName = config.getString(CassandraConfig.Keys.CASSANDRA_KEYNAME);
            this.keyspace = config.getString(CassandraConfig.Keys.CASSANDRA_KEYSPACE);
            return this;
        }

        CassandraSink<K, V> build() {
            return new CassandraSink<>(this);
        }
    }
}
