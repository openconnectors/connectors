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

package org.openconnectors.rabbitmq;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.openconnectors.config.Config;
import org.openconnectors.connect.ConnectorContext;
import org.openconnectors.connect.PushSourceConnector;
import org.openconnectors.rabbitmq.source.RabbitMQConsumer;
import org.openconnectors.rabbitmq.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.function.Consumer;

import static org.openconnectors.config.ConfigUtils.verifyExists;
import static org.openconnectors.rabbitmq.RabbitMQConfigKeys.*;

public class RabbitMQSourceConnector implements PushSourceConnector<byte[]> {

    private static Logger logger = LoggerFactory.getLogger(RabbitMQSourceConnector.class);

    private Consumer<Collection<byte[]>> consumer;
    private Connection rabbitMQConnection;
    private Channel rabbitMQChannel;

    @Override
    public void setConsumer(Consumer<Collection<byte[]>> consumeFunction) {
        if (consumeFunction == null) {
            throw new IllegalArgumentException("consumeFunction cannot be null.");
        }
        this.consumer = consumeFunction;
    }

    @Override
    public void initialize(ConnectorContext ctx) {
        // nothing to do
    }

    @Override
    public void open(Config config) throws Exception {
        validateConfig(config);
        String connectionUri = config.getString(AMQ_URI);
        String connectionName = config.getString(CONNECTION_NAME);
        String queueName = config.getString(QUEUE_NAME);
        if (consumer == null) {
            throw new IllegalArgumentException("consumer cannot be null.");
        }
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setUri(connectionUri);
        rabbitMQConnection = connectionFactory.newConnection(connectionName);
        logger.info("A new connection to {}:{} has been opened successfully.",
                rabbitMQConnection.getAddress().getCanonicalHostName(),
                rabbitMQConnection.getPort()
        );
        rabbitMQChannel = rabbitMQConnection.createChannel();
        rabbitMQChannel.queueDeclare(queueName, false, false, false, null);
        com.rabbitmq.client.Consumer consumer = new RabbitMQConsumer(this.consumer, rabbitMQChannel);
        rabbitMQChannel.basicConsume(queueName, consumer);
        logger.info("A consumer for queue {} has been successfully started.", queueName);
    }

    @Override
    public void close() throws Exception {
        rabbitMQChannel.close();
        rabbitMQConnection.close();
    }

    @Override
    public String getVersion() {
        return Version.getVersion();
    }

    private void validateConfig(Config config) {
        verifyExists(config, AMQ_URI);
        verifyExists(config, QUEUE_NAME);
    }
}
