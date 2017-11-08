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

package org.openconnectors.connect;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * Sink abstract connector meant to be at the end of DAG pipeline
 * i.e write data to persistent store
 *
 * @param <U> Type for messages to be submitted for publishing
 */
public interface SinkConnector<U> extends Connector {

    /**
     * Attempt to publish a type safe collection of messages
     *
     * @param messages Objects to process in the sink
     * @return Completable future fo async publish request
     */
    CompletableFuture<Void> publish(final Collection<U> messages);

    /**
     * Explicit flush useful in being called before closing the sink
     *
     * @throws Exception Generally IO type exceptions if flush fails
     */
    void flush() throws Exception;
}
