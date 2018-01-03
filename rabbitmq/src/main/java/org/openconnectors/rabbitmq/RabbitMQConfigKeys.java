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


/**
 * Contains the configuration keys for RabbitMQ connectors.
 */
public class RabbitMQConfigKeys {

    /**
     * The client-provided connection name (an arbitrary string).
     * Will be displayed in management UI if the server supports it.
     * The parameter is optional.
     */
    public static final String CONNECTION_NAME = "rabbitmq.connection.name";

    /**
     * The AMQP URI containing the settings of a RabbitMQ connection:
     * host, port, username, password and virtual host.
     * @see <a href="https://www.rabbitmq.com/uri-spec.html">RabbitMQ URI Specification</a>
     */
    public static final String AMQ_URI = "rabbitmq.connection_factory.amq_uri";

    /**
     * The name of the queue.
     */
    public static final String QUEUE_NAME = "rabbitmq.queue_name";


}
