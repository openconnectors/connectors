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

package org.openconnectors.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Value;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.util.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import static org.openconnectors.aerospike.AeroSpikeConfig.AEROSPIKE_CONNECTOR_VERSION;
import static org.openconnectors.aerospike.AeroSpikeConfig.Defaults;

/**
 * Simple AeroSpike sink
 */
public class AeroSpikeSink<K, V> implements SinkConnector<KeyValue<K, V>> {
    private static final long serialVersionUID = 6387089772307974259L;
    private static final Logger LOG = LoggerFactory.getLogger(AeroSpikeSink.class.getName());
    private final String keyspace, columnName, keySet;
    private final WritePolicy writePolicy;
    private AerospikeClient client;


    @Override
    public void initialize(ConnectorContext ctx) {
    }

    @Override
    public void open() throws Exception {
    }

    @Override
    public void close() throws Exception {
        client.close();
        LOG.info("Connection Closed");
    }

    @Override
    public String getVersion() {
        return AEROSPIKE_CONNECTOR_VERSION;
    }

    @Override
    public CompletableFuture<Void> publish(Collection<KeyValue<K, V>> tuples) {
        for (KeyValue<K, V> tuple : tuples) {
            Key key = new Key(keyspace, keySet, tuple.getKey().toString());
            Bin bin = new Bin(columnName, Value.getAsBlob(tuple.getValue()));
            client.put(writePolicy, key, bin);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() {
    }

    private Host[] getAerospikeHosts(String seedHosts) {
        String[] hosts = seedHosts.split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid Seed Hosts");
        }

        Host[] aeroSpikeHosts = new Host[hosts.length];

        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            Host aeroSpikeHost = new Host(hostPort[0], Integer.valueOf(hostPort[1]));
            aeroSpikeHosts[i] = aeroSpikeHost;
        }
        return aeroSpikeHosts;
    }

    private AeroSpikeSink(Builder builder) {
        String seedHosts = builder.seedHosts;
        keyspace = builder.keyspace;
        columnName = builder.columnName;
        keySet = builder.keySet;
        writePolicy = builder.writePolicy;
        String user = builder.user;
        String password = builder.password;
        ClientPolicy clientPolicy = new ClientPolicy();
        clientPolicy.user = user;
        clientPolicy.password = password;
        client = new AerospikeClient(clientPolicy, getAerospikeHosts(seedHosts));
    }

    public static final class Builder {
        private String seedHosts, keyspace, columnName, keySet, user, password;
        private int maxRetries, writeTimeoutMs;
        private WritePolicy writePolicy;

        private Builder() {
            this.maxRetries = Defaults.MAX_RETRIES;
            this.writeTimeoutMs = Defaults.WRITE_TIMEOUT_MS;
            this.keySet = Defaults.KEY_SET;
        }

        public Builder setSeedHosts(String seedHosts) {
            this.seedHosts = seedHosts;
            return this;
        }

        public Builder setKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder setColumnName(String columnName) {
            this.columnName = columnName;
            return this;
        }

        public Builder setUser(String user) {
            this.user = user;
            return this;
        }

        public Builder setPassword(String password) {
            this.password = password;
            return this;
        }

        public Builder setMaxRetries(int maxRetries) {
            this.maxRetries = maxRetries;
            return this;
        }

        public Builder setWriteTimeoutMs(int writeTimeoutMs) {
            this.writeTimeoutMs = writeTimeoutMs;
            return this;
        }

        public AeroSpikeSink build() {
            this.writePolicy = new WritePolicy();
            writePolicy.maxRetries = maxRetries;
            writePolicy.setTimeout(writeTimeoutMs);
            return new AeroSpikeSink(this);
        }
    }
}
