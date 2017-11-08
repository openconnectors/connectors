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

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

import com.aerospike.client.*;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.WritePolicy;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.util.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.openconnectors.aerospike.AeroSpikeConfigKeys.*;

/**
 * Simple AeroSpike sink
 */
public class AeroSpikeSink<K, V> implements SinkConnector<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(AeroSpikeSink.class);

    // ----- Runtime fields
    private AerospikeClient client;
    private String keySpace;
    private String keySet;
    private String columnName;
    private WritePolicy writePolicy;

    @Override
    public void initialize(ConnectorContext ctx) {
        // Nothing really
    }

    @Override
    public void open(Config config) throws Exception {
        verifyExists(config, AEROSPIKE_SEEDHOSTS);
        verifyExists(config, AEROSPIKE_KEYSPACE);
        verifyExists(config, AEROSPIKE_COLUMNNAME);
        keySpace = config.getString(AEROSPIKE_KEYSPACE);
        keySet = config.getString(AEROSPIKE_KEYSET, "");
        columnName = config.getString(AEROSPIKE_COLUMNNAME);
        writePolicy = new WritePolicy();
        writePolicy.maxRetries = config.getInt(AEROSPIKE_MAXRETRIES, 1);
        writePolicy.setTimeout(config.getInt(AEROSPIKE_WRITETIMEOUTMS, 100));
        createClient(config);
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    @Override
    public String getVersion() {
        return AEROSPIKE_CONNECTOR_VERSION;
    }

    @Override
    public CompletableFuture<Void> publish(Collection<KeyValue<K, V>> tuples) {
        for (KeyValue<K, V> tuple : tuples) {
            Key key = new Key(keySpace, keySet, tuple.getKey().toString());
            Bin bin = new Bin(columnName, Value.getAsBlob(tuple.getValue()));
            client.put(writePolicy, key, bin);
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
        String[] hosts = config.getString(AEROSPIKE_SEEDHOSTS).split(",");
        if (hosts.length <= 0) {
            throw new RuntimeException("Invalid seedhosts");
        }
        Host[] aerospikeHosts = new Host[hosts.length];
        for (int i = 0; i < hosts.length; ++i) {
            String[] hostPort = hosts[i].split(":");
            aerospikeHosts[i] = new Host(hostPort[0], Integer.valueOf(hostPort[1]));
        }
        ClientPolicy policy = new ClientPolicy();
        if (!config.getString(AEROSPIKE_USERNAME, "").isEmpty()
                && !config.getString(AEROSPIKE_PASSWORD, "").isEmpty()) {
            policy.user = config.getString(AEROSPIKE_USERNAME);
            policy.password = config.getString(AEROSPIKE_PASSWORD);
        }
        client = new AerospikeClient(policy, aerospikeHosts);
    }
}
