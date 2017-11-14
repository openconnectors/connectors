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

package org.openconnectors.elasticsearch;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.SinkConnector;
import org.openconnectors.util.KeyValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.openconnectors.config.ConfigUtils.verifyExists;

/**
 * Simple Kafka Sink to publish string messages to a topic
 */
public class KafkaSink010<K, V> implements SinkConnector<KeyValue<K, V>> {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink010.class);

    private Producer<K, V> producer;
    private Properties props = new Properties();
    private String topic;

    @Override
    public CompletableFuture<Void> publish(Collection<KeyValue<K, V>> messages) {
        ProducerRecord<K, V> record;
        for (KeyValue<K, V> tuple : messages) {
            record = new ProducerRecord<>(topic, tuple.getKey(), tuple.getValue());
            LOG.debug("Message sending to kafka, record={}.", record);
            producer.send(record);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void flush() throws Exception {
        producer.flush();
    }

    @Override
    public void close() throws IOException {
        producer.close();
        LOG.info("Kafka sink stopped.");
    }

    @Override
    public String getVersion() {
        return KafkaConnectorVersion010.getVersion();
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        // Nothing for now
    }

    @Override
    public void open(Config config) throws Exception {

        verifyExists(config, ConfigKeys.KAFKA_SINK_TOPIC);
        verifyExists(config, ConfigKeys.KAFKA_SINK_BOOTSTRAP_SERVERS);
        verifyExists(config, ConfigKeys.KAFKA_SINK_ACKS);
        verifyExists(config, ConfigKeys.KAFKA_SINK_BATCH_SIZE);
        verifyExists(config, ConfigKeys.KAFKA_SINK_MAX_REQUEST_SIZE);

        topic = config.getString(ConfigKeys.KAFKA_SINK_TOPIC);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString(ConfigKeys.KAFKA_SINK_BOOTSTRAP_SERVERS));
        props.put(ProducerConfig.ACKS_CONFIG, config.getString(ConfigKeys.KAFKA_SINK_ACKS));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.getLong(ConfigKeys.KAFKA_SINK_BATCH_SIZE).toString());
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getLong(ConfigKeys.KAFKA_SINK_MAX_REQUEST_SIZE).toString());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);

        LOG.info("Kafka sink started.");

    }
}
