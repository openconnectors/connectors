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
import org.openconnectors.config.Config;
import org.openconnectors.config.ConfigUtils;
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

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource010.class);

    private Consumer<String, V> consumer;
    private Properties props;
    private String topic;
    private Boolean autoCommitEnabled;

    private java.util.function.Consumer<Collection<V>> consumeFunction;

    @Override
    public void open(Config config) throws Exception {
        ConfigUtils.verifyExists(
                config,
                ConfigKeys.KAFKA_SOURCE_TOPIC,
                ConfigKeys.KAFKA_SINK_BOOTSTRAP_SERVERS,
                ConfigKeys.KAFKA_SOURCE_GROUP_ID,
                ConfigKeys.KAFKA_SOURCE_FETCH_MIN_BYTES,
                ConfigKeys.KAFKA_SOURCE_AUTO_COMMIT_INTERVAL_MS,
                ConfigKeys.KAFKA_SOURCE_SESSION_TIMEOUT_MS,
                ConfigKeys.KAFKA_SOURCE_AUTO_COMMIT_ENABLED
        );

        props = new Properties();

        topic = config.getString(ConfigKeys.KAFKA_SOURCE_TOPIC);
        autoCommitEnabled = config.getBoolean(ConfigKeys.KAFKA_SOURCE_AUTO_COMMIT_ENABLED);

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(ConfigKeys.KAFKA_SOURCE_BOOTSTRAP_SERVERS));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString(ConfigKeys.KAFKA_SOURCE_GROUP_ID));
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, config.getInt(ConfigKeys.KAFKA_SOURCE_FETCH_MIN_BYTES).toString());
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, config.getLong(ConfigKeys.KAFKA_SOURCE_AUTO_COMMIT_INTERVAL_MS).toString());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getLong(ConfigKeys.KAFKA_SOURCE_SESSION_TIMEOUT_MS).toString());

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        this.start();

    }

    @Override
    public void close() throws IOException {
        LOG.info("Stopping kafka source");
        if(consumer != null) {
            consumer.close();
        }
        LOG.info("Kafka source stopped.");
    }

    public void start() throws Exception {
        Thread runnerThread = new Thread(() -> {
            LOG.info("Starting kafka source");
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            LOG.info("Kafka source started.");
            ConsumerRecords<String, V> records;
            List<V> values = new ArrayList<>();
            while(true){
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
    public String getVersion() {
        return KafkaConnectorVersion010.getVersion();
    }

    @Override
    public void setConsumer(java.util.function.Consumer<Collection<V>> consumeFunction) {
        this.consumeFunction = consumeFunction;
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        // Nothing for now.
    }

}
