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

public class ConfigKeys {
    public static final String KAFKA_SINK_BOOTSTRAP_SERVERS = "kafka.sink.bootstrap_servers";
    public static final String KAFKA_SINK_ACKS = "kafka.sink.acks";
    public static final String KAFKA_SINK_BATCH_SIZE = "kafka.sink.batch_size";
    public static final String KAFKA_SINK_MAX_REQUEST_SIZE = "kafka.sink.max_request_size";
    public static final String KAFKA_SINK_TOPIC = "kafka.sink.topic";
    public static final String KAFKA_SOURCE_BOOTSTRAP_SERVERS = "kafka.source.bootstrap_servers";
    public static final String KAFKA_SOURCE_GROUP_ID = "kafka.source.group_id";
    public static final String KAFKA_SOURCE_FETCH_MIN_BYTES = "kafka.source.fetch_min_bytes";
    public static final String KAFKA_SOURCE_AUTO_COMMIT_INTERVAL_MS = "kafka.source.auto_commit_interval_ms";
    public static final String KAFKA_SOURCE_SESSION_TIMEOUT_MS = "kafka.source.session_timeout_ms";
}
