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

import com.twitter.heron.streamlet.Context;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.Sink;
import org.openconnectors.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Simple Heron Cassandra Sink
 */
public class HeronCassandraSink<K, V> implements Sink<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(HeronCassandraSink.class);
    private CassandraSink<K, V> sink;

    @Override
    public void setup(Context ctx) {
        if (sink == null) {
            sink = new CassandraSink<>();
        }
        try {
            sink.open(new ConfigProvider());
        } catch (Exception e) {
            throw new RuntimeException("Exception during setup of Aerospike Sink", e);
        }
    }

    @Override
    public void put(KeyValue<K, V> tuple) {
        sink.publish(Collections.singleton(new org.openconnectors.util.KeyValue<>(tuple.getKey(), tuple.getValue())));
    }

    @Override
    public void cleanup() {
        try {
            sink.close();
        } catch (Exception e) {
            LOG.error("Exception thrown while closing Aerospike Sink", e);
        }
    }
}