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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

/**
 * Simple Kafka Source to emit strng messages from a topic
 */
public class KafkaSource010<V> implements PushSourceConnector<V> {
    private static final long serialVersionUID = -6255843583086784051L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource010.class.getName());
    private final String topic;
    private final boolean autoCommitEnabled;
    private final Properties props;
    private java.util.function.Consumer<Collection<V>> consumeFunction;

    private Consumer<String, V> consumer;

    @Override
    public void open() throws Exception {
        Thread runnerThread = new Thread(() -> {
            LOG.info("Starting kafka source");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            LOG.info("Kafka source started.");
            ConsumerRecords<String, V> records;
            List<V> values = new ArrayList<>();

            while (true){
                records = consumer.poll(1000);
                values.clear();
                for (ConsumerRecord<String, V> record : records) {
                    LOG.debug("Message received from kafka, key: {}. value: {}", record.key(), record.value());
                    values.add(record.value());
                }
                consumeFunction.accept(values);
                if (!autoCommitEnabled) {
                    consumer.commitSync();
                }
            }

        });
        runnerThread.setName("Kafka Source Thread");
        runnerThread.start();
    }

    @Override
    public void close() throws IOException {
        LOG.info("Stopping kafka source");
        if(consumer != null) {
            consumer.close();
        }
        LOG.info("Kafka source stopped.");
    }

    @Override
    public String getVersion() {
        return KafkaConfig.KAFKA_CONNECTOR_VERSION;
    }

    @Override
    public void setConsumer(java.util.function.Consumer<Collection<V>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void initialize(ConnectorContext ctx) {
    }

    private KafkaSource010(Builder<V> builder) {
        topic = builder.topic;
        autoCommitEnabled = builder.autoCommitEnabled;
        props = builder.props;
        setConsumer(builder.consumeFunction);
    }

    public static class Builder<V> {
        private String topic;
        private boolean autoCommitEnabled;
        private Properties props;
        private java.util.function.Consumer<Collection<V>> consumeFunction;

        private Builder() {
            autoCommitEnabled = KafkaConfig.Defaults.AUTO_COMMIT_ENABLED;
            props = new Properties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfig.Defaults.KEY_SERIALIZER_CLASS);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfig.Defaults.VALUE_SERIALIZER_CLASS);
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public Builder setGroupId(String groupId) {
            props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            return this;
        }

        public Builder setAutoCommitEnabled(boolean autoCommitEnabled) {
            this.autoCommitEnabled = autoCommitEnabled;
            return this;
        }

        public Builder setMinBytes(int minBytes) {
            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, minBytes);
            return this;
        }

        public Builder setAutoCommitIntervalMillis(long autoCommitIntervalMillis) {
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, autoCommitIntervalMillis);
            return this;
        }

        public Builder setSessionTimeoutMillis(long sessionTimeoutMillis) {
            props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMillis);
            return this;
        }

        public Builder setConsumeFunction(java.util.function.Consumer<Collection<V>> consumeFunction) {
            this.consumeFunction = consumeFunction;
            return this;
        }

        public Builder usingConfigProvider(Config config) {
            config.verify(
                    KafkaConfig.Keys.KAFKA_SOURCE_TOPIC,
                    KafkaConfig.Keys.KAFKA_SOURCE_GROUP_ID,
                    KafkaConfig.Keys.KAFKA_SOURCE_FETCH_MIN_BYTES
            );
            topic = config.getString(KafkaConfig.Keys.KAFKA_SOURCE_TOPIC);
            props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(KafkaConfig.Keys.KAFKA_SOURCE_GROUP_ID));
            props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.getLong(KafkaConfig.Keys.KAFKA_SOURCE_FETCH_MIN_BYTES));
            return this;
        }

        public KafkaSource010 build() {
            Arrays.asList(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ConsumerConfig.GROUP_ID_CONFIG,
                    ConsumerConfig.FETCH_MIN_BYTES_CONFIG,
                    ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
                    ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG
            ).forEach(prop -> {
                if (props.getProperty(prop) == null) {
                    throw new IllegalArgumentException(String.format("Property %s is missing from the config", prop));
                }
            });

            return new KafkaSource010<>(this);
        }
    }
}
