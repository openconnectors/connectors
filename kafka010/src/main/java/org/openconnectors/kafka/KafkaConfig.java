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

package org.openconnectors.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

public final class KafkaConfig {
    static class Keys {
        private static final String KAFKA_SINK_PREFIX = "kafka.sink.";
        private static final String KAFKA_SOURCE_PREFIX = "kafka.source.";

        // Sink/consumer configs
        static final String KAFKA_SINK_BOOTSTRAP_SERVERS = KAFKA_SINK_PREFIX + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
        static final String KAFKA_SINK_ACKS = KAFKA_SINK_PREFIX + ProducerConfig.ACKS_CONFIG;
        static final String KAFKA_SINK_BATCH_SIZE = KAFKA_SINK_PREFIX + ProducerConfig.BATCH_SIZE_CONFIG;
        static final String KAFKA_SINK_MAX_REQUEST_SIZE = KAFKA_SINK_PREFIX + ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
        static final String KAFKA_SINK_TOPIC = KAFKA_SINK_PREFIX + "topic";

        // Source/producer configs
        static final String KAFKA_SOURCE_BOOTSTRAP_SERVERS = KAFKA_SOURCE_PREFIX + ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
        static final String KAFKA_SOURCE_GROUP_ID = KAFKA_SOURCE_PREFIX + ConsumerConfig.GROUP_ID_CONFIG;
        static final String KAFKA_SOURCE_FETCH_MIN_BYTES = KAFKA_SOURCE_PREFIX + ConsumerConfig.FETCH_MIN_BYTES_CONFIG;
        static final String KAFKA_SOURCE_AUTO_COMMIT_INTERVAL_MS = KAFKA_SOURCE_PREFIX + ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG;
        static final String KAFKA_SOURCE_SESSION_TIMEOUT_MS = KAFKA_SOURCE_PREFIX + ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG;
        static final String KAFKA_SOURCE_AUTO_COMMIT_ENABLED = KAFKA_SOURCE_PREFIX + "auto_commit_enabled";
        static final String KAFKA_SOURCE_TOPIC= KAFKA_SOURCE_PREFIX + "topic";
    }

    static class Defaults {
        static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
        static final String VALUE_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
        static final boolean AUTO_COMMIT_ENABLED = false;
    }

    static final String KAFKA_CONNECTOR_VERSION = "kafka.io.connector-0.0.1";
}
