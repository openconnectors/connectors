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
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

/**
 * Simple Kafka Sink to publish string messages to a topic
 */
public class KafkaSink010<K, V> implements SinkConnector<KeyValue<K, V>> {
    private static final long serialVersionUID = 1858275615268645616L;
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSink010.class.getName());
    private final String topic;
    private Producer<K, V> producer;

    @Override
    public CompletableFuture<Void> publish(Collection<KeyValue<K, V>> messages) {
        ProducerRecord<K, V> record;
        for (KeyValue<K, V> keyValue : messages) {
            record = new ProducerRecord<>(topic, keyValue.getKey(), keyValue.getValue());
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
        LOG.info("Kafka sink stopped");
    }

    @Override
    public String getVersion() {
        return KafkaConfig.KAFKA_CONNECTOR_VERSION;
    }

    @Override
    public void initialize(ConnectorContext ctx) {
    }

    @Override
    public void open() {
    }

    private KafkaSink010(Builder builder) {
        topic = builder.topic;
        producer = new KafkaProducer<>(builder.props);
    }

    public static final class Builder {
        private Properties props;
        private String topic;

        private Builder() {
            props = new Properties();
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConfig.Defaults.KEY_SERIALIZER_CLASS);
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaConfig.Defaults.VALUE_SERIALIZER_CLASS);
        }

        public Builder setTopic(String topic) {
            this.topic = topic;
            return this;
        }

        public Builder setBootstrapServers(String bootstrapServers) {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            return this;
        }

        public Builder setAcks(String acksConfig) {
            props.put(ProducerConfig.ACKS_CONFIG, acksConfig);
            return this;
        }

        public Builder setBatchSize(long batchSize) {
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize);
            return this;
        }

        public Builder setMaxRequestSize(long maxRequestSize) {
            props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, maxRequestSize);
            return this;
        }

        public Builder usingConfigProvider(Config config) {
            config.verify(
                    KafkaConfig.Keys.KAFKA_SINK_BOOTSTRAP_SERVERS,
                    KafkaConfig.Keys.KAFKA_SINK_ACKS,
                    KafkaConfig.Keys.KAFKA_SINK_BATCH_SIZE,
                    KafkaConfig.Keys.KAFKA_SINK_MAX_REQUEST_SIZE
            );
            setBootstrapServers(config.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
            setAcks(config.getString(ProducerConfig.ACKS_CONFIG));
            setBatchSize(config.getLong(ProducerConfig.BATCH_SIZE_CONFIG));
            setMaxRequestSize(config.getLong(ProducerConfig.MAX_REQUEST_SIZE_CONFIG));
            return this;
        }

        public KafkaSink010 build() {
            Arrays.asList(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                    ProducerConfig.ACKS_CONFIG,
                    ProducerConfig.BATCH_SIZE_CONFIG,
                    ProducerConfig.MAX_REQUEST_SIZE_CONFIG
            ).forEach(prop -> {
                if (props.getProperty(prop) == null) {
                    throw new IllegalArgumentException(String.format("Property %s is missing from the config", prop));
                }
            });

            return new KafkaSink010(this);
        }
    }
}
